export function requireInternalServiceToken(
  env: NodeJS.ProcessEnv = process.env,
) {
  const token = env.INTERNAL_SERVICE_TOKEN;
  if (!token || token.length < 32)
    throw new Error("INTERNAL_SERVICE_TOKEN must contain at least 32 characters");
  return token;
}
