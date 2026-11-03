export async function request(path, options = {}) {
  const response = await fetch("/api/v1" + path, {
    ...options,
    headers: { "Content-Type": "application/json", ...options.headers },
  });
  if (response.status === 204) return null;
  const body = await response.json();
  if (!response.ok)
    throw new Error(
      body.detail || "Le service est momentanément indisponible.",
    );
  return body;
}
export const send = (path, body, method = "POST") =>
  request(path, {
    method,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
