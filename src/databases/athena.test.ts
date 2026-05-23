import {
  beforeEach,
  describe,
  expect,
  test,
  vi,
  MockInstance,
} from "vitest";
import athena, {
  AthenaConfig,
  AthenaQueryCancelledError,
  AthenaQueryFailedError
} from './athena.js';
import * as athenaModule from "@aws-sdk/client-athena";
import {
  AthenaClient,
  AthenaError,
  ColumnInfo,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
  GetQueryResultsCommandInput,
  GetQueryResultsOutput,
  QueryExecution,
  StartQueryExecutionCommand,
} from "@aws-sdk/client-athena";

vi.mock("@aws-sdk/client-athena", async (importOriginal) => {
  const originalModule = await importOriginal();
  const send = vi.fn().mockRejectedValue(new Error("mock not configured"));
  const AthenaClient = vi.fn(() => ({send}));
  return {...(originalModule as unknown as Record<string, unknown>), AthenaClient, send};
});

type TestContext = {
  config: AthenaConfig;
  send: MockInstance<AthenaClient["send"]>;
  queryExecutionStates: string[];
  athenaError: AthenaError;
  queryResultsPages: GetQueryResultsOutput[];
}

function createResultPages(metadata: ColumnInfo[], pages: string[][][]): GetQueryResultsOutput[] {
  return pages.map((page, index) => {
    const rows = page.map((row) => {
      return {Data: row.map((value) => ({VarCharValue: value}))}
    })
    if (index === 0) {
      rows.unshift({Data: metadata.map((ci) => ({VarCharValue: ci.Name!}))})
    }
    return {
      ResultSet: {
        ResultSetMetadata: {
          ColumnInfo: metadata,
        },
        Rows: rows,
      },
      NextToken: index < pages.length - 1 ? `page-${index + 1}` : undefined,
    };
  });
}

function findCommand<T>(send: MockInstance<AthenaClient["send"]>, type: new (...args: any[]) => T): T | undefined {
  const call = send.mock.calls.find((call) => call[0] instanceof type);
  return call !== undefined ? call[0] as T : undefined;
}

describe("Athena DatabaseClient", () => {
  beforeEach<TestContext>((context) => {
    vi.clearAllMocks();
    context.config = {
      type: "athena",
      authType: "iam-credentials",
      accessKeyId: "TESTACCESSKEY",
      secretAccessKey: "abcdefg0123456",
    };
    context.queryExecutionStates = ["SUCCEEDED"];
    context.queryResultsPages = createResultPages([], [[]]);
    const send = (athenaModule as unknown as {send: AthenaClient["send"]}).send;
    if (vi.isMockFunction(send)) {
      send.mockImplementation((command) => {
        if (command instanceof StartQueryExecutionCommand) {
          return Promise.resolve({QueryExecutionId: "query-123"});
        } else if (command instanceof GetQueryExecutionCommand) {
          const state = context.queryExecutionStates.shift() ?? "SUCCEEDED";
          const queryExecution = {Status: {State: state}} as QueryExecution;
          if (state === "FAILED" && context.athenaError !== undefined) {
            queryExecution.Status!.AthenaError = context.athenaError;
          }
          return Promise.resolve({QueryExecution: queryExecution});
        } else if (command instanceof GetQueryResultsCommand) {
          const page = context.queryResultsPages.shift() ?? {NextToken: undefined};
          return Promise.resolve(page);
        } else {
          return Promise.reject(new Error("Unexpected command"));
        }
      });
      context.send = send;
    }
  });

  test<TestContext>("creates an AthenaClient with credentials", async ({config}) => {
    const queryFn = athena(config);
    await queryFn`SELECT 1`;
    expect(AthenaClient).toHaveBeenCalledWith({
      credentials: {
        accessKeyId: "TESTACCESSKEY",
        secretAccessKey: "abcdefg0123456",
      },
    });
  });

  test<TestContext>("passes region to AthenaClient when set", async ({config}) => {
    config.region = "us-west-2";
    const queryFn = athena(config);
    await queryFn`SELECT 1`;
    expect(AthenaClient).toHaveBeenCalledWith(
      expect.objectContaining({region: "us-west-2"})
    );
  });

  test<TestContext>("does not set region when not in config", async ({config}) => {
    delete config.region;
    const queryFn = athena(config);
    await queryFn`SELECT 1`;
    expect(AthenaClient).toHaveBeenCalledWith(
      expect.not.objectContaining({region: expect.anything()})
    );
  });

  test<TestContext>("sends a StartQueryExecutionCommand with the SQL string", async ({config, send}) => {
    const queryFn = athena(config);
    await queryFn`SELECT * FROM my_table`;
    const command = findCommand(send, StartQueryExecutionCommand);
    expect(command!.input).toHaveProperty("QueryString", "SELECT * FROM my_table");
  });

  test<TestContext>("sends a parameterized query with string parameters", async ({config, send}) => {
    const queryFn = athena(config);
    await queryFn`SELECT * FROM users WHERE name = ${"Alice"} AND city = ${"Paris"}`;
    const command = findCommand(send, StartQueryExecutionCommand);
    expect(command!.input).toHaveProperty("QueryString", "SELECT * FROM users WHERE name = ? AND city = ?");
    expect(command!.input).toHaveProperty("ExecutionParameters", ["'Alice'", "'Paris'"]);
  });

  test<TestContext>("sends boolean and number parameters as unquoted strings", async ({config, send}) => {
    const queryFn = athena(config);
    await queryFn`SELECT * FROM t WHERE active = ${true} AND count > ${42} AND rate = ${3.14} AND big = ${9007199254740993n}`;
    const command = findCommand(send, StartQueryExecutionCommand);
    expect(command!.input).toHaveProperty("ExecutionParameters", ["true", "42", "3.14", "9007199254740993"]);
  });

  test<TestContext>("sends null and undefined parameters as NULL", async ({config, send}) => {
    const queryFn = athena(config);
    await queryFn`SELECT * FROM t WHERE a = ${null} AND b = ${undefined}`;
    const command = findCommand(send, StartQueryExecutionCommand);
    expect(command!.input).toHaveProperty("ExecutionParameters", ["NULL", "NULL"]);
  });

  test<TestContext>("sends NaN and Infinity as NUMBER literals", async ({config, send}) => {
    const queryFn = athena(config);
    await queryFn`SELECT ${NaN}, ${Infinity}, ${-Infinity}`;
    const command = findCommand(send, StartQueryExecutionCommand);
    expect(command!.input).toHaveProperty("ExecutionParameters", ["NUMBER 'NaN'", "NUMBER 'Infinity'", "NUMBER '-Infinity'"]);
  });

  test<TestContext>("sends array parameters as ARRAY literals with recursive conversion", async ({config, send}) => {
    const queryFn = athena(config);
    await queryFn`SELECT ${["hello", "world"]}, ${[1, 2, 3]}`;
    const command = findCommand(send, StartQueryExecutionCommand);
    expect(command!.input).toHaveProperty("ExecutionParameters", ["ARRAY['hello', 'world']", "ARRAY[1, 2, 3]"]);
  });

  test<TestContext>("sends Date parameters as TIMESTAMP literals in UTC", async ({config, send}) => {
    const queryFn = athena(config);
    await queryFn`SELECT * FROM t WHERE created_at > ${new Date("2024-03-15T10:30:00.123Z")}`;
    const command = findCommand(send, StartQueryExecutionCommand);
    expect(command!.input).toHaveProperty("ExecutionParameters", ["TIMESTAMP '2024-03-15 10:30:00.123'"]);
  });

  test<TestContext>("sends unknown parameter types as quoted string literals", async ({config, send}) => {
    const queryFn = athena(config);
    await queryFn`SELECT * FROM t WHERE data = ${{foo: "bar"}} AND other = ${/regex/}`;
    const command = findCommand(send, StartQueryExecutionCommand);
    expect(command!.input).toHaveProperty("ExecutionParameters", ["'[object Object]'", "'/regex/'"]);
  });

  test<TestContext>("escapes single quotes in string and unknown parameters", async ({config, send}) => {
    const queryFn = athena(config);
    const objWithQuote = {toString() { return "it's complex"; }};
    await queryFn`SELECT * FROM t WHERE name = ${"O'Brien"} AND data = ${objWithQuote}`;
    const command = findCommand(send, StartQueryExecutionCommand);
    expect(command!.input).toHaveProperty("ExecutionParameters", ["'O''Brien'", "'it''s complex'"]);
  });

  test<TestContext>("sends WorkGroup when set in config", async ({config, send}) => {
    config.workGroup = "my-workgroup";
    const queryFn = athena(config);
    await queryFn`SELECT 1`;
    const command = findCommand(send, StartQueryExecutionCommand);
    expect(command!.input).toHaveProperty("WorkGroup", "my-workgroup");
  });

  test<TestContext>("does not send WorkGroup when not set in config", async ({config, send}) => {
    delete config.workGroup;
    const queryFn = athena(config);
    await queryFn`SELECT 1`;
    const command = findCommand(send, StartQueryExecutionCommand);
    expect(command!.input).not.toHaveProperty("WorkGroup");
  });

  test<TestContext>("sends QueryExecutionContext with database and catalog when set in config", async ({config, send}) => {
    config.database = "my_database";
    config.catalog = "my_catalog";
    const queryFn = athena(config);
    await queryFn`SELECT 1`;
    const command = findCommand(send, StartQueryExecutionCommand);
    expect(command!.input).toHaveProperty("QueryExecutionContext", {
      Database: "my_database",
      Catalog: "my_catalog",
    });
  });

  test<TestContext>("does not send QueryExecutionContext when neither database nor catalog is set", async ({config, send}) => {
    const queryFn = athena(config);
    await queryFn`SELECT 1`;
    const command = findCommand(send, StartQueryExecutionCommand);
    expect(command!.input).not.toHaveProperty("QueryExecutionContext");
  });

  test<TestContext>("calls GetQueryExecution with the returned query execution ID", async ({config, send}) => {
    const queryFn = athena(config);
    await queryFn`SELECT * FROM my_table`;
    const command = findCommand(send, GetQueryExecutionCommand);
    expect(command!.input).toHaveProperty("QueryExecutionId", "query-123");
  });

  test<TestContext>("polls GetQueryExecution until state is SUCCEEDED", async (context) => {
    context.queryExecutionStates = ["RUNNING", "QUEUED", "SUCCEEDED"];
    const queryFn = athena(context.config);
    await queryFn`SELECT 1`;
    const calls = context.send.mock.calls.filter((call) => call[0] instanceof GetQueryExecutionCommand);
    expect(calls).toHaveLength(3);
  });

  test<TestContext>("throws an AthenaQueryFailedError exposing the errorCategory, errorType, and retryable properties from the response", async (context) => {
    context.queryExecutionStates = ["FAILED"];
    context.athenaError = {
      ErrorMessage: "Some error",
      ErrorCategory: 2,
      ErrorType: 1005,
      Retryable: true,
    };
    const queryFn = athena(context.config);
    const error = await queryFn`SELECT 1`.catch((e) => e);
    expect(error).toBeInstanceOf(AthenaQueryFailedError);
    expect(error.message).toBe("Some error");
    expect(error.errorCategory).toBe(2);
    expect(error.errorType).toBe(1005);
    expect(error.retryable).toBeTruthy();
  });

  test<TestContext>("throws an AthenaQueryCancelledError when query is cancelled", async (context) => {
    context.queryExecutionStates = ["RUNNING", "CANCELLED"];
    const queryFn = athena(context.config);
    await expect(queryFn`SELECT 1`).rejects.toThrow(AthenaQueryCancelledError);
  });

  test<TestContext>("calls GetQueryResults with the query execution ID after query succeeds", async (context) => {
    const queryFn = athena(context.config);
    await queryFn`SELECT * FROM test_data`;
    const command = findCommand(context.send, GetQueryResultsCommand);
    expect(command!.input).toHaveProperty("QueryExecutionId", "query-123");
  });

  test<TestContext>("paginates GetQueryResults until NextToken is undefined", async (context) => {
    context.queryResultsPages = createResultPages([], [[], [], []]);
    const queryFn = athena(context.config);
    await queryFn`SELECT * FROM test_data`;
    const commandInputs = context.send.mock.calls
      .filter((call) => call[0] instanceof GetQueryResultsCommand)
      .map((call) => call[0].input) as GetQueryResultsCommandInput[];
    expect(commandInputs).toHaveLength(3);
    expect(commandInputs[0].NextToken).toBeUndefined();
    expect(commandInputs[1].NextToken).toBe("page-1");
    expect(commandInputs[2].NextToken).toBe("page-2");
  });

  test<TestContext>("returns schema from the first page of results", async (context) => {
    const metadata: ColumnInfo[] = [
      {Name: "col_boolean", Type: "boolean"},
      {Name: "col_tinyint", Type: "tinyint"},
      {Name: "col_smallint", Type: "smallint"},
      {Name: "col_int", Type: "int"},
      {Name: "col_integer", Type: "integer"},
      {Name: "col_bigint", Type: "bigint"},
      {Name: "col_float", Type: "float"},
      {Name: "col_real", Type: "real"},
      {Name: "col_double", Type: "double"},
      {Name: "col_decimal", Type: "decimal"},
      {Name: "col_char", Type: "char"},
      {Name: "col_varchar", Type: "varchar"},
      {Name: "col_string", Type: "string"},
      {Name: "col_binary", Type: "binary"},
      {Name: "col_varbinary", Type: "varbinary"},
      {Name: "col_date", Type: "date"},
      {Name: "col_timestamp", Type: "timestamp"},
      {Name: "col_timestamp_tz", Type: "timestamp with time zone"},
      {Name: "col_array", Type: "array"},
      {Name: "col_map", Type: "map"},
      {Name: "col_struct", Type: "struct"},
      {Name: "col_row", Type: "row"},
      {Name: "col_json", Type: "json"},
      {Name: "col_nullable", Type: "varchar", Nullable: "NULLABLE"},
      {Name: "col_not_null", Type: "varchar", Nullable: "NOT_NULL"},
      {Name: "col_unknown", Type: "varchar", Nullable: "UNKNOWN"},
    ];
    context.queryResultsPages = createResultPages(metadata, [[]]);
    const queryFn = athena(context.config);
    const result = await queryFn`SELECT * FROM test_data`;
    expect(result.schema).toEqual([
      {name: "col_boolean", type: "boolean"},
      {name: "col_tinyint", type: "integer"},
      {name: "col_smallint", type: "integer"},
      {name: "col_int", type: "integer"},
      {name: "col_integer", type: "integer"},
      {name: "col_bigint", type: "bigint"},
      {name: "col_float", type: "number"},
      {name: "col_real", type: "number"},
      {name: "col_double", type: "number"},
      {name: "col_decimal", type: "number"},
      {name: "col_char", type: "string"},
      {name: "col_varchar", type: "string"},
      {name: "col_string", type: "string"},
      {name: "col_binary", type: "buffer"},
      {name: "col_varbinary", type: "buffer"},
      {name: "col_date", type: "date"},
      {name: "col_timestamp", type: "date"},
      {name: "col_timestamp_tz", type: "date"},
      {name: "col_array", type: "array"},
      {name: "col_map", type: "object"},
      {name: "col_struct", type: "object"},
      {name: "col_row", type: "object"},
      {name: "col_json", type: "object"},
      {name: "col_nullable", type: "string", nullable: true},
      {name: "col_not_null", type: "string", nullable: false},
      {name: "col_unknown", type: "string"},
    ]);
  });

  test<TestContext>("returns rows from all pages, skipping the header row on the first page", async (context) => {
    const metadata = [
      {Name: "name", Type: "varchar"},
      {Name: "city", Type: "varchar"},
    ];
    const pages = [
      [
        ["Alice", "Paris"],
        ["Bob", "London"],
      ],
      [
        ["Charlie", "Berlin"],
      ],
    ];
    context.queryResultsPages = createResultPages(metadata, pages);
    const queryFn = athena(context.config);
    const result = await queryFn`SELECT * FROM test_data`;
    expect(result.rows).toEqual([
      {name: "Alice", city: "Paris"},
      {name: "Bob", city: "London"},
      {name: "Charlie", city: "Berlin"},
    ]);
  });

  test<TestContext>("converts boolean, integer, and numeric values", async (context) => {
    const metadata = [
      {Name: "bool_col", Type: "boolean"},
      {Name: "int_col", Type: "integer"},
      {Name: "bigint_col", Type: "bigint"},
      {Name: "float_col", Type: "float"},
      {Name: "double_col", Type: "double"},
      {Name: "decimal_col", Type: "decimal"},
    ];
    const pages = [
      [
        ["true", "42", "9007199254740993", "3.14", "2.718281828", "123.45"],
        ["false", "-1", "123", "0.0", "-99.9", "0.001"],
      ]
    ];
    context.queryResultsPages = createResultPages(metadata, pages);
    const queryFn = athena(context.config);
    const result = await queryFn`SELECT * FROM test_data`;
    expect(result.rows).toEqual([
      {bool_col: true, int_col: 42, bigint_col: 9007199254740993n, float_col: 3.14, double_col: 2.718281828, decimal_col: 123.45},
      {bool_col: false, int_col: -1, bigint_col: 123n, float_col: 0.0, double_col: -99.9, decimal_col: 0.001},
    ]);
  });

  test<TestContext>("converts binary and varbinary values from hex-encoded strings to Buffers", async (context) => {
    const metadata = [
      {Name: "bin_col", Type: "binary"},
      {Name: "varbin_col", Type: "varbinary"},
    ];
    const pages = [[["a1 f3 54", "00 ff 80"]]];
    context.queryResultsPages = createResultPages(metadata, pages);
    const queryFn = athena(context.config);
    const result = await queryFn`SELECT * FROM test_data`;
    expect(result.rows).toEqual([
      {bin_col: Buffer.from([0xa1, 0xf3, 0x54]), varbin_col: Buffer.from([0x00, 0xff, 0x80])},
    ]);
  });

  test<TestContext>("converts date values to Date objects", async (context) => {
    const metadata = [{Name: "date_col", Type: "date"}];
    const pages = [[["2024-03-15"]]];
    context.queryResultsPages = createResultPages(metadata, pages);
    const queryFn = athena(context.config);
    const result = await queryFn`SELECT * FROM test_data`;
    expect(result.rows).toEqual([
      {date_col: new Date("2024-03-15")},
    ]);
  });

  test<TestContext>("converts timestamp values to Date objects", async (context) => {
    const metadata = [
      {Name: "ts_no_frac", Type: "timestamp"},
      {Name: "ts_millis", Type: "timestamp"},
      {Name: "ts_nanos", Type: "timestamp"},
    ];
    const pages = [
      [
        ["2024-03-15 10:30:00", "2024-03-15 10:30:00.123", "2024-03-15 10:30:00.123456789"],
      ]
    ];
    context.queryResultsPages = createResultPages(metadata, pages);
    const queryFn = athena(context.config);
    const result = await queryFn`SELECT * FROM test_data`;
    expect(result.rows).toEqual([{
      ts_no_frac: new Date("2024-03-15T10:30:00"),
      ts_millis: new Date("2024-03-15T10:30:00.123"),
      ts_nanos: new Date("2024-03-15T10:30:00.123"),
    }]);
  });

  test<TestContext>("converts timestamp with time zone values to Date objects", async (context) => {
    const metadata = [
      {Name: "ts_utc", Type: "timestamp with time zone"},
      {Name: "ts_named", Type: "timestamp with time zone"},
      {Name: "ts_offset_pos", Type: "timestamp with time zone"},
      {Name: "ts_offset_neg", Type: "timestamp with time zone"},
    ];
    const pages = [
      [
        ["2024-03-15 10:30:00.000 UTC", "2024-03-15 10:30:00.000 America/New_York", "2024-03-15 10:30:00.000 +04:30", "2024-03-15 10:30:00.000 -05:00"],
      ]
    ];
    context.queryResultsPages = createResultPages(metadata, pages);
    const queryFn = athena(context.config);
    const result = await queryFn`SELECT * FROM test_data`;
    expect(result.rows).toEqual([{
      ts_utc: new Date("2024-03-15T10:30:00.000Z"),
      ts_named: new Date("2024-03-15T14:30:00.000Z"),
      ts_offset_pos: new Date("2024-03-15T06:00:00.000Z"),
      ts_offset_neg: new Date("2024-03-15T15:30:00.000Z"),
    }]);
  });

  test<TestContext>("converts JSON values using JSON.parse", async (context) => {
    const metadata = [{Name: "json_col", Type: "json"}];
    const pages = [
      [
        ['{"name":"Alice","age":30}'],
        ['[1,2,3]'],
        ['"hello"'],
        ['42'],
        ['true'],
        ['null'],
      ]
    ];
    context.queryResultsPages = createResultPages(metadata, pages);
    const queryFn = athena(context.config);
    const result = await queryFn`SELECT * FROM test_data`;
    expect(result.rows).toEqual([
      {json_col: {name: "Alice", age: 30}},
      {json_col: [1, 2, 3]},
      {json_col: "hello"},
      {json_col: 42},
      {json_col: true},
      {json_col: null},
    ]);
  });

  test<TestContext>("converts array values", async (context) => {
    const metadata = [{Name: "arr_col", Type: "array"}];
    const pages = [
      [
        ["[hello, world]"],
        ["[nested, [a, b]]"],
      ]
    ];
    context.queryResultsPages = createResultPages(metadata, pages);
    const queryFn = athena(context.config);
    const result = await queryFn`SELECT * FROM test_data`;
    expect(result.rows).toEqual([
      {arr_col: ["hello", "world"]},
      {arr_col: ["nested", ["a", "b"]]},
    ]);
  });

  test<TestContext>("converts map, struct, and row values", async (context) => {
    const metadata = [
      {Name: "map_col", Type: "map"},
      {Name: "struct_col", Type: "struct"},
      {Name: "row_col", Type: "row"},
    ];
    const pages = [
      [
        ["{foo=bar, baz=qux}", "{name=Alice, city=Paris}", "{x=1, y=2}"],
      ]
    ];
    context.queryResultsPages = createResultPages(metadata, pages);
    const queryFn = athena(context.config);
    const result = await queryFn`SELECT * FROM test_data`;
    expect(result.rows).toEqual([
      {
        map_col: {foo: "bar", baz: "qux"},
        struct_col: {name: "Alice", city: "Paris"},
        row_col: {x: "1", y: "2"},
      },
    ]);
  });

  test<TestContext>("converts struct with nested values", async (context) => {
    const metadata = [{Name: "nested_col", Type: "struct"}];
    context.queryResultsPages = createResultPages(metadata, [[["{tags=[a, b], meta={x=1}}"]]]);
    const queryFn = athena(context.config);
    const result = await queryFn`SELECT * FROM test_data`;
    expect(result.rows).toEqual([
      {nested_col: {tags: ["a", "b"], meta: {x: "1"}}},
    ]);
  });

  test<TestContext>("converts struct with unnamed keys as array", async (context) => {
    const metadata = [{Name: "unnamed_col", Type: "struct"}];
    context.queryResultsPages = createResultPages(metadata, [[["{1, 2, 3}"]]]);
    const queryFn = athena(context.config);
    const result = await queryFn`SELECT * FROM test_data`;
    expect(result.rows).toEqual([
      {unnamed_col: ["1", "2", "3"]},
    ]);
  });

  test<TestContext>("maps unknown types to other and returns values as strings", async (context) => {
    const metadata = [
      {Name: "uuid_col", Type: "uuid"},
      {Name: "ip_col", Type: "ipaddress"},
    ];
    const pages = [
      [
        ["550e8400-e29b-41d4-a716-446655440000", "192.168.1.1"]
      ]
    ];
    context.queryResultsPages = createResultPages(metadata, pages);
    const queryFn = athena(context.config);
    const result = await queryFn`SELECT * FROM test_data`;
    expect(result.schema).toEqual([
      {name: "uuid_col", type: "other"},
      {name: "ip_col", type: "other"},
    ]);
    expect(result.rows).toEqual([
      {uuid_col: "550e8400-e29b-41d4-a716-446655440000", ip_col: "192.168.1.1"},
    ]);
  });

  test<TestContext>("returns the date the query ran, and the runtime duration", async (context) => {
    const originalImpl = context.send.getMockImplementation();
    if (originalImpl !== undefined) {
      context.send.mockImplementation(async (command) => {
        await new Promise((resolve) => setTimeout(resolve, 1));
        return originalImpl(command, {}, () => {});
      });
    }
    const before = Date.now();
    const queryFn = athena(context.config);
    const result = await queryFn`SELECT * FROM test_data`;
    const after = Date.now();
    expect(result.date.getTime()).toBeGreaterThanOrEqual(before);
    expect(result.date.getTime()).toBeLessThanOrEqual(after);
    expect(result.duration).toBeGreaterThan(0);
    expect(result.duration).toBeLessThanOrEqual(after - before);
  });
});
