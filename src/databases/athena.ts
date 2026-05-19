import {
  AthenaClient as AthenaSdkClient,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
  StartQueryExecutionCommand,
} from "@aws-sdk/client-athena";
import type {
  AthenaError as AthenaErrorInfo,
  ColumnInfo,
  GetQueryResultsCommandInput,
  Row,
  StartQueryExecutionCommandInput,
} from "@aws-sdk/client-athena";
import type {
  AwsCredentialIdentity,
  AwsCredentialIdentityProvider
} from "@smithy/types";
import type {QueryTemplateFunction, SerializableQueryResult} from "./index.js";
import type {ColumnSchema} from "../runtime/index.js";

type AthenaIamCredentialsAuth = {
  authType: "iam-credentials",
  accessKeyId: string;
  secretAccessKey: string;
};

export type AthenaConfig = {
  type: "athena";
  region?: string;
  workGroup?: string;
  catalog?: string;
  database?: string;
} & AthenaIamCredentialsAuth;

export default function athena(config: AthenaConfig): QueryTemplateFunction {
  return async (strings, ...params) => {
    const startedAt = new Date();
    const client = new AthenaClient(config);
    const {schema, rows} = await client.execute(strings, params);
    const duration = Date.now() - startedAt.getTime();
    return {schema, rows, duration, date: startedAt};
  };
};

class AthenaClient {
  private client: AthenaSdkClient;
  private workGroup?: string;
  private catalog?: string;
  private database?: string;

  constructor(config: AthenaConfig) {
    this.client = new AthenaSdkClient({
      ...config.region && {region: config.region},
      credentials: createCredentialsProvider(config),
    });
    this.workGroup = config.workGroup;
    this.catalog = config.catalog;
    this.database = config.database;
  }

  public async execute(queryStringPieces: readonly string[], rawExecutionParameters: readonly unknown[]): Promise<Pick<SerializableQueryResult, 'schema' | 'rows'>> {
    const queryString = queryStringPieces.join("?");
    const executionParameters = rawExecutionParameters.map(toAthenaLiteral);
    const queryExecutionId = await this.startQuery(queryString, executionParameters);
    await this.awaitQuery(queryExecutionId);
    return this.loadResults(queryExecutionId);
  }

  private async startQuery(queryString: string, executionParameters: string[]): Promise<string> {
    const parameters: StartQueryExecutionCommandInput = {QueryString: queryString};
    if (executionParameters.length > 0) {
      parameters.ExecutionParameters = executionParameters;
    }
    if (this.workGroup) {
      parameters.WorkGroup = this.workGroup;
    }
    if (this.database || this.catalog) {
      parameters.QueryExecutionContext = {
        ...this.database && {Database: this.database},
        ...this.catalog && {Catalog: this.catalog},
      };
    }
    const response = await this.client.send(new StartQueryExecutionCommand(parameters));
    return response.QueryExecutionId!;
  }

  private async awaitQuery(queryExecutionId: string): Promise<void> {
    while (true) {
      const parameters = {QueryExecutionId: queryExecutionId};
      const response = await this.client.send(new GetQueryExecutionCommand(parameters));
      const state = response.QueryExecution?.Status?.State;
      if (state === "FAILED") {
        throw new AthenaQueryFailedError(response.QueryExecution?.Status?.AthenaError);
      } else if (state === "CANCELLED") {
        throw new AthenaQueryCancelledError();
      } else if (state === "SUCCEEDED") {
        break;
      }
    }
  }

  private async loadResults(queryExecutionId: string): Promise<Pick<SerializableQueryResult, 'schema' | 'rows'>> {
    let nextToken: string | undefined = undefined;
    let schema: ColumnSchema[] = [];
    const rows: Record<string, unknown>[] = [];
    do {
      const parameters: GetQueryResultsCommandInput = {QueryExecutionId: queryExecutionId};
      if (nextToken !== undefined) {
        parameters.NextToken = nextToken
      }
      const response = await this.client.send(new GetQueryResultsCommand(parameters));
      const columnInfo = response.ResultSet?.ResultSetMetadata?.ColumnInfo ?? [];
      const dataRows = (response.ResultSet?.Rows ?? []);
      if (nextToken === undefined) {
        schema = columnInfo.map(toColumnSchema);
        dataRows.shift();
      }
      rows.push(...dataRows.map((rawRow) => toRow(columnInfo, rawRow)));
      nextToken = response.NextToken;
    } while (nextToken !== undefined);
    return {schema, rows};
  }
}

function toAthenaLiteral(value: unknown): string {
  if (typeof value === "string") {
    return `'${value}'`;
  } else if (value instanceof Date) {
    return `TIMESTAMP '${value.toISOString().replace("T", " ").slice(0, 23)}'`;
  } else {
    return String(value);
  }
}

function toColumnSchema(column: ColumnInfo): ColumnSchema {
  const schema: ColumnSchema = {
    name: column.Name!,
    type: (CONVERTERS[column.Type!] || OTHER_CONVERTER).type,
  };
  if (column.Nullable === "NOT_NULL") {
    schema.nullable = false;
  } else if (column.Nullable === "NULLABLE") {
    schema.nullable = true;
  }
  return schema;
}

function toRow(columnInfo: ColumnInfo[], rawRow: Row): Record<string, unknown> {
  return Object.fromEntries((rawRow.Data || []).map((datum, index) => {
    const name = columnInfo[index].Name!;
    const type = columnInfo[index].Type!;
    if (datum.VarCharValue != undefined) {
      const converter = CONVERTERS[type] || OTHER_CONVERTER;
      return [name, converter.convert(datum.VarCharValue, type)];
    } else {
      return [name, undefined];
    }
  }));
}

export class AthenaError extends Error {
  constructor(message?: string) {
    super(message);
  }
}

export class AthenaQueryFailedError extends AthenaError {
  public errorCategory?: number;
  public errorType?: number;
  public retryable?: boolean

  constructor(athenaError?: AthenaErrorInfo) {
    super(athenaError?.ErrorMessage);
    this.errorCategory = athenaError?.ErrorCategory;
    this.errorType = athenaError?.ErrorType;
    this.retryable = athenaError?.Retryable;
  }
}

export class AthenaQueryCancelledError extends AthenaError {
  constructor() {
    super("Query cancelled by user")
  }
}

function createCredentialsProvider(config: AthenaConfig): AwsCredentialIdentity | AwsCredentialIdentityProvider {
  return {accessKeyId: config.accessKeyId, secretAccessKey: config.secretAccessKey}
}

type TypeConverter = {
  type: ColumnSchema["type"],
  convert: (value: string, athenaType: string) => boolean | number | string | bigint | object,
};

const BOOLEAN_CONVERTER: TypeConverter = {
  type: "boolean",
  convert(value) {
    return value === "true";
  },
};

const INTEGER_CONVERTER: TypeConverter = {
  type: "integer",
  convert(value) {
    return parseInt(value, 10);
  },
};

const BIGINT_CONVERTER: TypeConverter = {
  type: "bigint",
  convert(value) {
    return BigInt(value);
  },
};

const NUMBER_CONVERTER: TypeConverter = {
  type: "number",
  convert(value) {
    return parseFloat(value);
  },
};

const DATE_CONVERTER: TypeConverter = {
  type: "date",
  convert(value) {
    return new Date(value);
  }
};

const TIMESTAMP_CONVERTER: TypeConverter = {
  type: "date",
  convert(value, athenaType) {
    const [date, time, timeZone] = value.split(" ");
    const isoDateTime = `${date}T${time.slice(0, 12)}`;
    if (athenaType === "timestamp") {
      return new Date(isoDateTime);
    } else if (timeZone === "UTC" || timeZone === "Z") {
      return new Date(`${isoDateTime}Z`);
    } else if (/^[+-]\d{2}:\d{2}$/.test(timeZone)) {
      return new Date(`${isoDateTime}${timeZone}`);
    } else {
      const dateInTargetTimezone = new Date(isoDateTime + 'Z');
      const utcDate = new Date(dateInTargetTimezone.toLocaleString('en-US', {timeZone}));
      const offset = utcDate.getTime() - dateInTargetTimezone.getTime();
      const localDate = new Date(isoDateTime);
      const correctTimestamp = localDate.getTime() - offset;
      return new Date(correctTimestamp);
    }
  }
};

const BINARY_CONVERTER: TypeConverter = {
  type: "buffer",
  convert(value) {
    return Buffer.from(value.split(" ").map((b) => parseInt(b, 16)));
  },
};

const COMPLEX_CONVERTER = {
  convert(value: string): ReturnType<TypeConverter['convert']> {
    return this.parse(value);
  },
  split(value: string): string[] {
    const parts: string[] = [];
    let depth = 0;
    let current = "";
    for (let i = 0; i < value.length; i++) {
      const ch = value[i];
      if (ch === "[" || ch === "{") {
        depth++;
        current += ch;
      } else if (ch === "]" || ch === "}") {
        depth--;
        current += ch;
      } else if (ch === "," && depth === 0 && value[i + 1] === " ") {
        parts.push(current);
        current = "";
        i++;
      } else {
        current += ch;
      }
    }
    if (current) {
      parts.push(current);
    }
    return parts;
  },
  parse(value: string): string | object {
    if (value.startsWith("[") && value.endsWith("]")) {
      return this.parseArray(value);
    } else if (value.startsWith("{") && value.endsWith("}")) {
      return this.parseStruct(value);
    } else {
      return value;
    }
  },
  parseArray(value: string): (string | object)[] {
    const inner = value.slice(1, -1);
    if (inner === "") return [];
    return this.split(inner).map(this.parse.bind(this));
  },
  parseStruct(value: string): Record<string, string | object> | (string | object)[] {
    const inner = value.slice(1, -1);
    if (inner === "") return {};
    const parts = this.split(inner);
    if (parts.every((p) => !p.includes("=") || p.startsWith("[") || p.startsWith("{"))) {
      return parts.map(this.parse.bind(this));
    } else {
      return Object.fromEntries(parts.map((part) => {
        const eqIndex = part.indexOf("=");
        const key = part.slice(0, eqIndex);
        const val = part.slice(eqIndex + 1);
        return [key, this.parse(val)];
      }));
    }
  },
};

const ARRAY_CONVERTER: TypeConverter = {
  type: "array",
  ...COMPLEX_CONVERTER,
};

const OBJECT_CONVERTER: TypeConverter = {
  type: "object",
  ...COMPLEX_CONVERTER,
};

const JSON_CONVERTER: TypeConverter = {
  type: "object",
  convert(value) {
    return JSON.parse(value);
  },
};

const STRING_CONVERTER: TypeConverter = {
  type: "string",
  convert(value) {
    return value;
  },
};

const OTHER_CONVERTER: TypeConverter = {
  ...STRING_CONVERTER,
  type: "other",
};

const CONVERTERS: Record<string, TypeConverter> = {
  "array": ARRAY_CONVERTER,
  "bigint": BIGINT_CONVERTER,
  "binary": BINARY_CONVERTER,
  "boolean": BOOLEAN_CONVERTER,
  "char": STRING_CONVERTER,
  "date": DATE_CONVERTER,
  "decimal": NUMBER_CONVERTER,
  "double": NUMBER_CONVERTER,
  "float": NUMBER_CONVERTER,
  "int": INTEGER_CONVERTER,
  "integer": INTEGER_CONVERTER,
  "json": JSON_CONVERTER,
  "map": OBJECT_CONVERTER,
  "real": NUMBER_CONVERTER,
  "row": OBJECT_CONVERTER,
  "smallint": INTEGER_CONVERTER,
  "string": STRING_CONVERTER,
  "struct": OBJECT_CONVERTER,
  "timestamp with time zone": TIMESTAMP_CONVERTER,
  "timestamp": TIMESTAMP_CONVERTER,
  "tinyint": INTEGER_CONVERTER,
  "varbinary": BINARY_CONVERTER,
  "varchar": STRING_CONVERTER,
};
