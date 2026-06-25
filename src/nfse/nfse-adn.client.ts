import { Injectable, Logger } from '@nestjs/common';
import { request as httpsRequest, Agent } from 'https';
import { readFileSync } from 'fs';
import { gunzipSync } from 'zlib';
import { NfseCertService } from './nfse-cert.service';

/**
 * Cliente HTTP para as APIs de Distribuição dos Contribuintes do ADN
 * (Ambiente de Dados Nacional) do Sistema Nacional NFS-e.
 *
 * Autenticação: mTLS com certificado ICP-Brasil A1 (e-CNPJ). O certificado
 * É a autenticação (não há token). O CNPJ Raiz do certificado precisa bater
 * com o CNPJ consultado. O cert vem do banco (vinculado pelo frontend) ou,
 * em fallback, do .pfx apontado por NFSE_ADN_CERT_PFX_PATH.
 *
 * Métodos oficiais (Manual dos Contribuintes - Guia das APIs do ADN, v1.0):
 *   GET /DFe/{NSU}                  -> distribuição incremental por NSU
 *   GET /NFSe/{ChaveAcesso}/Eventos -> eventos de uma chave de acesso
 *
 * IMPORTANTE: os NOMES dos campos do JSON de resposta vêm do Swagger que fica
 * atrás do mTLS. Por isso o parse aqui é DEFENSIVO. Ao validar em produção
 * restrita, confira os nomes reais (o serviço loga as chaves do 1º documento).
 */

export interface AdnDfeItem {
  nsu: number;
  chaveAcesso?: string;
  tipoDocumento?: string;
  dataHoraGeracao?: string;
  /** XML já descompactado (gunzip do gzip+base64, ou base64 puro). */
  xml: string;
  /** Objeto bruto retornado pela ADN, p/ inspeção/ajuste de mapeamento. */
  raw: any;
}

export interface AdnDfeResposta {
  status: number;
  ultimoNSU: number;
  maxNSU: number;
  documentos: AdnDfeItem[];
  /** true quando a ADN diz que não há documentos a partir do NSU (404/E2220 ou 204). */
  semDocumentos: boolean;
  statusProcessamento?: string;
  erros?: any[];
  rawBody: string;
}

@Injectable()
export class NfseAdnClient {
  private readonly logger = new Logger(NfseAdnClient.name);
  private agent: Agent | null = null;
  private agentCnpj: string | null = null;

  constructor(private readonly cert: NfseCertService) {}

  /** Invalida o agente mTLS (chamar quando o certificado é trocado). */
  resetAgent() {
    this.agent = null;
    this.agentCnpj = null;
  }

  /** Agente mTLS com o e-CNPJ A1. Tenta o cert do banco; senão, .pfx do env. */
  private async getAgent(cnpjConsulta?: string): Promise<Agent> {
    const alvo = (cnpjConsulta || process.env.NFSE_ADN_CNPJ || '').replace(/\D/g, '');
    if (this.agent && this.agentCnpj === alvo) return this.agent;

    const db = await this.cert.obterAtivo(alvo);
    if (db) {
      this.agent = new Agent({ pfx: db.pfx, passphrase: db.passphrase, keepAlive: true });
    } else {
      const pfxPath = process.env.NFSE_ADN_CERT_PFX_PATH;
      if (!pfxPath) {
        throw new Error(
          'Nenhum certificado vinculado (banco) nem NFSE_ADN_CERT_PFX_PATH configurado.',
        );
      }
      this.agent = new Agent({
        pfx: readFileSync(pfxPath),
        passphrase: process.env.NFSE_ADN_CERT_PASSPHRASE || undefined,
        keepAlive: true,
      });
    }
    this.agentCnpj = alvo;
    return this.agent;
  }

  private baseUrl(): string {
    return (
      process.env.NFSE_ADN_BASE_URL ||
      'https://adn.producaorestrita.nfse.gov.br/contribuintes'
    ).replace(/\/+$/, '');
  }

  private danfseBaseUrl(): string {
    return (
      process.env.NFSE_DANFSE_BASE_URL ||
      'https://adn.producaorestrita.nfse.gov.br/danfse'
    ).replace(/\/+$/, '');
  }

  /**
   * GET /DFe/{NSU} — retorna os DF-e a partir do NSU informado.
   * cnpjConsulta: opcional; CNPJ (mesma raiz) diferente do CNPJ do certificado.
   */
  async consultarDFe(nsu: number, cnpjConsulta?: string): Promise<AdnDfeResposta> {
    const cnpj = (cnpjConsulta || process.env.NFSE_ADN_CNPJ || '').replace(/\D/g, '');
    const qs = cnpj ? `?cnpj=${encodeURIComponent(cnpj)}` : '';
    const { status, body } = await this.get(this.baseUrl(), `/DFe/${nsu}${qs}`, cnpj);
    return this.parseDfe(status, body, nsu);
  }

  /** GET /NFSe/{ChaveAcesso}/Eventos — eventos vinculados a uma chave. */
  async consultarEventos(
    chaveAcesso: string,
    cnpjConsulta?: string,
  ): Promise<{ status: number; documentos: AdnDfeItem[]; rawBody: string }> {
    const { status, body } = await this.get(
      this.baseUrl(),
      `/NFSe/${encodeURIComponent(chaveAcesso)}/Eventos`,
      cnpjConsulta,
    );
    const parsed = this.parseDfe(status, body, 0);
    return { status, documentos: parsed.documentos, rawBody: body };
  }

  /**
   * Baixa o PDF do DANFSE pela chave de acesso (API DANFSE do ADN).
   * O path exato pode variar conforme o Swagger; ajuste via NFSE_DANFSE_PATH
   * (use {chave} como placeholder). Default: /{chave}.
   */
  async baixarDanfse(chaveAcesso: string, cnpjConsulta?: string): Promise<Buffer> {
    const tpl = process.env.NFSE_DANFSE_PATH || '/{chave}';
    const path = tpl.replace('{chave}', encodeURIComponent(chaveAcesso));
    const { status, buffer } = await this.getBuffer(this.danfseBaseUrl(), path, cnpjConsulta);
    if (status !== 200 || !buffer.length) {
      throw new Error(`DANFSE indisponível (status ${status}) para a chave ${chaveAcesso}.`);
    }
    return buffer;
  }

  private async get(
    base: string,
    path: string,
    cnpj?: string,
  ): Promise<{ status: number; body: string }> {
    const { status, buffer } = await this.getBuffer(base, path, cnpj, 'application/json');
    return { status, body: buffer.toString('utf8') };
  }

  private async getBuffer(
    base: string,
    path: string,
    cnpj?: string,
    accept = 'application/pdf',
  ): Promise<{ status: number; buffer: Buffer }> {
    const url = new URL(base + path);
    const agent = await this.getAgent(cnpj);
    const timeoutMs = Number(process.env.NFSE_ADN_TIMEOUT_MS) || 30000;
    return new Promise((resolve, reject) => {
      const req = httpsRequest(
        {
          method: 'GET',
          hostname: url.hostname,
          port: url.port || 443,
          path: url.pathname + url.search,
          agent,
          headers: { Accept: accept },
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on('data', (c) => chunks.push(c as Buffer));
          res.on('end', () => resolve({ status: res.statusCode || 0, buffer: Buffer.concat(chunks) }));
        },
      );
      req.on('error', reject);
      req.setTimeout(timeoutMs, () => req.destroy(new Error('timeout ao chamar a ADN')));
      req.end();
    });
  }

  private parseDfe(status: number, body: string, nsuConsultado: number): AdnDfeResposta {
    const base = (extra: Partial<AdnDfeResposta> = {}): AdnDfeResposta => ({
      status,
      ultimoNSU: nsuConsultado,
      maxNSU: nsuConsultado,
      documentos: [],
      semDocumentos: false,
      rawBody: body,
      ...extra,
    });

    // 204 = sem documentos novos a partir do NSU informado.
    if (status === 204 || !body) return base({ semDocumentos: true });

    let json: any = {};
    try {
      json = JSON.parse(body);
    } catch {
      this.logger.warn(`Resposta da ADN não é JSON válido (status ${status}).`);
      return base();
    }

    // Contrato real da ADN: StatusProcessamento / LoteDFe / Erros / Alertas.
    const statusProc = json.StatusProcessamento || json.statusProcessamento;
    const erros: any[] = json.Erros || json.erros || [];
    const semDocumentos =
      statusProc === 'NENHUM_DOCUMENTO_LOCALIZADO' ||
      erros.some((e) => (e?.Codigo || e?.codigo) === 'E2220');

    const lote: any[] =
      json.LoteDFe || json.loteDFe || json.lote || json.Documentos || json.documentos || [];
    const documentos = (Array.isArray(lote) ? lote : [])
      .map((item) => this.mapItem(item))
      .filter((d): d is AdnDfeItem => !!d);

    const maxNsuDoc = documentos.length ? Math.max(...documentos.map((d) => d.nsu)) : nsuConsultado;
    const ultimoNSU = Number(
      json.ultimoNSU ?? json.UltimoNSU ?? json.NsuMaximo ?? json.nsuMaximo ?? maxNsuDoc,
    );
    const maxNSU = Number(
      json.maxNSU ?? json.MaxNSU ?? json.NsuMaximo ?? json.nsuMaximo ?? json.ultimoNSU ?? ultimoNSU,
    );

    return {
      status,
      ultimoNSU,
      maxNSU,
      documentos,
      semDocumentos,
      statusProcessamento: statusProc,
      erros,
      rawBody: body,
    };
  }

  private mapItem(item: any): AdnDfeItem | null {
    if (!item) return null;
    const b64 =
      item.ArquivoXml || item.arquivoXml || item.documentoXml || item.xmlGZipB64 || item.xml;
    return {
      nsu: Number(item.NSU ?? item.nsu ?? 0),
      chaveAcesso: item.ChaveAcesso || item.chaveAcesso || item.chave,
      tipoDocumento: item.TipoDocumento || item.tipoDocumento || item.tipo,
      dataHoraGeracao:
        item.DataHoraGeracao || item.dataHoraGeracao || item.dataHoraRecebimento,
      xml: typeof b64 === 'string' && b64.length ? this.decodeXml(b64) : '',
      raw: item,
    };
  }

  /** O XML vem como gzip+base64; se não for gzip, cai para base64 puro. */
  private decodeXml(b64: string): string {
    const buf = Buffer.from(b64, 'base64');
    try {
      return gunzipSync(buf).toString('utf8');
    } catch {
      return buf.toString('utf8');
    }
  }
}
