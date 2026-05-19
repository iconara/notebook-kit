import {beforeEach, describe, test} from "vitest";
import athena, {AthenaConfig} from './athena.js';

type TestContext = {
  config: AthenaConfig;
}

describe("Athena DatabaseClient", () => {
  beforeEach<TestContext>((context) => {
    context.config = {
      type: "athena",
      authType: "iam-credentials",
      accessKeyId: "TESTACCESSKEY",
      secretAccessKey: "abcdefg0123456",
    };
  })

  test<TestContext>("returns a query function", ({expect, config}) => {
    const queryFn = athena(config);
    expect(queryFn).not.toBeUndefined();
  })
});
