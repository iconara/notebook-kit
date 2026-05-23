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
  if (value === null || value === undefined) {
    return "NULL";
  } else if (typeof value === "string") {
    return `'${value.replace(/'/g, "''")}'`;
  } else if (typeof value === "boolean" || typeof value === "bigint") {
    return String(value);
  } else if (typeof value === "number") {
    if (isNaN(value) || !isFinite(value)) return `NUMBER '${value}'`;
    return String(value);
  } else if (Array.isArray(value)) {
    return `ARRAY[${value.map(toAthenaLiteral).join(", ")}]`;
  } else if (value instanceof Date) {
    return `TIMESTAMP '${value.toISOString().replace("T", " ").slice(0, 23)}'`;
  } else {
    return toAthenaLiteral(String(value));
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

const COMPLEX_CONVERTER = {
  convert(value: string): ReturnType<TypeConverter['convert']> {
    if (value.startsWith("[") && value.endsWith("]")) {
      return this.parseArray(value);
    } else if (value.startsWith("{") && value.endsWith("}")) {
      return this.parseStruct(value);
    } else {
      return value;
    }
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
  parseArray(value: string) {
    const inner = value.slice(1, -1);
    if (inner === "") return [];
    return this.split(inner).map(this.convert.bind(this));
  },
  parseStruct(value: string) {
    const inner = value.slice(1, -1);
    if (inner === "") return {};
    const parts = this.split(inner);
    if (parts.every((p) => !p.includes("=") || p.startsWith("[") || p.startsWith("{"))) {
      return parts.map(this.convert.bind(this));
    } else {
      return Object.fromEntries(parts.map((part) => {
        const eqIndex = part.indexOf("=");
        const key = part.slice(0, eqIndex);
        const val = part.slice(eqIndex + 1);
        return [key, this.convert(val)];
      }));
    }
  },
};

const CONVERTERS: Record<string, TypeConverter> = {
  "array": {
    type: "array",
    ...COMPLEX_CONVERTER,
  },
  "bigint": {
    type: "bigint",
    convert: BigInt,
  },
  "boolean": {
    type: "boolean",
    convert: (v) => v === "true",
  },
  "date": {
    type: "date",
    convert: (v) => new Date(v),
  },
  "double": {
    type: "number",
    convert: (v) => parseFloat(v),
  },
  "integer": {
    type: "integer",
    convert: (v) => parseInt(v, 10),
  },
  "json": {
    type: "object",
    convert: (v) => JSON.parse(v),
  },
  "row": {
    type: "object",
    ...COMPLEX_CONVERTER,
  },
  "timestamp": {
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
  },
  "varbinary": {
    type: "buffer",
    convert: (v) => Buffer.from(v.split(" ").map((b) => parseInt(b, 16))),
  },
  "varchar": {
    type: "string",
    convert: (v) => v,
  },
};

CONVERTERS["char"] = CONVERTERS["string"] = CONVERTERS["varchar"];
CONVERTERS["tinyint"] = CONVERTERS["smallint"] = CONVERTERS["int"] = CONVERTERS["integer"];
CONVERTERS["binary"] = CONVERTERS["varbinary"];
CONVERTERS["decimal"] = CONVERTERS["float"] = CONVERTERS["real"] = CONVERTERS["double"];
CONVERTERS["timestamp with time zone"] = CONVERTERS["timestamp"];
CONVERTERS["map"] = CONVERTERS["struct"] = CONVERTERS["row"];

const OTHER_CONVERTER: TypeConverter = {
  ...CONVERTERS["varchar"],
  type: "other",
};
