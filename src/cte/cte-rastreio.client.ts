import { Injectable, Logger } from '@nestjs/common';
import { request as httpsRequest } from 'https';

/** Um evento de movimentação retornado pelo SSW (documento.tracking[]). */
export interface SswTrackingEvento {
  data_hora?: string;
  data_hora_efetiva?: string;
  dominio?: string;
  filial?: string;
  cidade?: string;
  ocorrencia?: string;
  descricao?: string;
  tipo?: string;
  nome_recebedor?: string;
  nro_doc_recebedor?: string;
  codigo_ssw?: string;
}

/** Resposta normalizada da API pública trackingdanfe. */
export interface SswTrackingResposta {
  success: boolean;
  message?: string;
  header?: {
    remetente?: string;
    destinatario?: string;
    nro_nf?: string;
    pedido?: string;
  };
  tracking: SswTrackingEvento[];
}

/**
 * Cliente da WebAPI pública de rastreamento do SSW (trackingdanfe).
 *
 *   POST https://ssw.inf.br/api/trackingdanfe
 *   body JSON: { "chave_nfe": "<44 dígitos da NF-e transportada>" }
 *
 * Sem autenticação. Resolve a transportadora pela chave da NF-e (mesmo em
 * subcontratação/redespacho). `success:false` quando a transportadora não usa SSW.
 *
 * Config por env:
 *  - SSW_TRACKING_URL: endpoint (default https://ssw.inf.br/api/trackingdanfe)
 *  - SSW_TRACKING_TIMEOUT_MS: timeout da requisição (default 20000)
 */
@Injectable()
export class CteRastreioClient {
  private readonly logger = new Logger(CteRastreioClient.name);

  private get url(): string {
    return process.env.SSW_TRACKING_URL || 'https://ssw.inf.br/api/trackingdanfe';
  }

  private get timeoutMs(): number {
    return Number(process.env.SSW_TRACKING_TIMEOUT_MS) || 20000;
  }

  /** Consulta o rastreio de uma NF-e transportada. Nunca lança: erros viram success=false. */
  async rastrearPorChaveNfe(chaveNfe: string): Promise<SswTrackingResposta> {
    const chave = String(chaveNfe || '').replace(/\D/g, '');
    if (chave.length !== 44) {
      return { success: false, message: 'Chave de NF-e inválida', tracking: [] };
    }

    try {
      const raw = await this.postJson({ chave_nfe: chave });
      return this.normalizar(raw);
    } catch (e) {
      this.logger.warn(
        `Falha ao consultar SSW (chave ${chave}): ${e instanceof Error ? e.message : String(e)}`,
      );
      return { success: false, message: 'Erro de comunicação com o SSW', tracking: [] };
    }
  }

  /** Mapeia o JSON cru do SSW para o shape estável usado pelo serviço. */
  private normalizar(raw: any): SswTrackingResposta {
    const success = raw?.success === true;
    if (!success) {
      return { success: false, message: raw?.message || 'Nenhum documento localizado', tracking: [] };
    }
    const doc = raw?.documento || {};
    const tracking: SswTrackingEvento[] = Array.isArray(doc?.tracking) ? doc.tracking : [];
    return {
      success: true,
      message: raw?.message,
      header: doc?.header || undefined,
      tracking,
    };
  }

  private postJson(body: Record<string, unknown>): Promise<any> {
    const payload = Buffer.from(JSON.stringify(body), 'utf-8');
    const u = new URL(this.url);

    return new Promise((resolve, reject) => {
      const req = httpsRequest(
        {
          method: 'POST',
          hostname: u.hostname,
          port: u.port || 443,
          path: u.pathname + u.search,
          headers: {
            'Content-Type': 'application/json',
            Accept: 'application/json',
            'Content-Length': payload.length,
          },
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on('data', (c) => chunks.push(c as Buffer));
          res.on('end', () => {
            const text = Buffer.concat(chunks).toString('utf-8');
            try {
              resolve(JSON.parse(text));
            } catch {
              reject(new Error(`Resposta não-JSON (HTTP ${res.statusCode}): ${text.slice(0, 120)}`));
            }
          });
        },
      );
      req.on('error', reject);
      req.setTimeout(this.timeoutMs, () => {
        req.destroy();
        reject(new Error(`Timeout (${this.timeoutMs}ms)`));
      });
      req.write(payload);
      req.end();
    });
  }
}
