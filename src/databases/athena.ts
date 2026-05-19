import {QueryTemplateFunction} from "./index.js";

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

export default function athena({type, ...options}: AthenaConfig): QueryTemplateFunction {
  return (strings: readonly string[], ...params: QueryParam[]) => Promise.reject(new Error("Not implemented"))
};
