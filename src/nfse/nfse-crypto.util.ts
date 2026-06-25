import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'crypto';

/**
 * Cifragem simétrica da senha do certificado (AES-256-GCM).
 *
 * A chave deriva de NFSE_CERT_SECRET (env). Sem o segredo, faz fallback para
 * texto puro — o serviço loga um aviso ao salvar nesse caso. Formato cifrado:
 *   enc:v1:<iv b64>:<tag b64>:<ciphertext b64>
 */
const PREFIX = 'enc:v1:';

function chave(): Buffer | null {
  const secret = process.env.NFSE_CERT_SECRET;
  if (!secret) return null;
  return createHash('sha256').update(secret).digest(); // 32 bytes
}

export function temSegredo(): boolean {
  return !!chave();
}

export function cifrar(texto: string): string {
  const key = chave();
  if (!key) return texto; // fallback: texto puro
  const iv = randomBytes(12);
  const cipher = createCipheriv('aes-256-gcm', key, iv);
  const ct = Buffer.concat([cipher.update(texto, 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return `${PREFIX}${iv.toString('base64')}:${tag.toString('base64')}:${ct.toString('base64')}`;
}

export function decifrar(valor: string): string {
  if (!valor?.startsWith(PREFIX)) return valor; // texto puro
  const key = chave();
  if (!key) throw new Error('NFSE_CERT_SECRET ausente para decifrar a senha do certificado.');
  const [, , ivB64, tagB64, ctB64] = valor.split(':');
  const decipher = createDecipheriv('aes-256-gcm', key, Buffer.from(ivB64, 'base64'));
  decipher.setAuthTag(Buffer.from(tagB64, 'base64'));
  return Buffer.concat([decipher.update(Buffer.from(ctB64, 'base64')), decipher.final()]).toString('utf8');
}
