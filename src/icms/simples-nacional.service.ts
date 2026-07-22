import { Injectable, Logger } from '@nestjs/common';
import { get as httpsGet } from 'https';

/** Resultado da consulta de regime tributário de um fornecedor (PJ). */
export interface RegimeFornecedor {
    /** true/false quando determinado via API; null quando indeterminado (CPF, API fora/limitada). */
    optanteSimples: boolean | null;
    fonte: 'receitaws' | 'indefinido';
    razaoSocial?: string;
}

/**
 * Consulta se o fornecedor é optante do Simples Nacional — sempre online,
 * sem persistência em banco (decisão de negócio: consultar toda vez).
 *
 * Provedor: ReceitaWS (mesmo do financeiro-service) — GET /v1/cnpj/{cnpj},
 * campos `simples.optante` e `simei.optante` (MEI conta como Simples).
 *
 * O plano gratuito limita ~3 req/min; para um lote de notas do mesmo
 * fornecedor não estourar o limite, há apenas uma memoização EM MEMÓRIA
 * (nada vai a banco; reinício do serviço zera). TTL via SIMPLES_MEMO_TTL_MS
 * (default 10 min; 0 desliga e consulta a cada chamada).
 *
 * Config por env:
 *  - SIMPLES_CONSULTA_URL_BASE: base da API (default https://www.receitaws.com.br/v1/cnpj)
 *  - SIMPLES_CONSULTA_TIMEOUT_MS: timeout da requisição (default 20000)
 *  - SIMPLES_MEMO_TTL_MS: memoização em memória (default 600000; 0 = desligada)
 *
 * Nunca lança: qualquer falha (429, timeout) devolve { optanteSimples: null }
 * — o chamador decide o fallback (CRT do XML da NF-e).
 */
@Injectable()
export class SimplesNacionalService {
    private readonly logger = new Logger(SimplesNacionalService.name);

    /** Memoização por processo p/ não estourar o rate limit dentro de um lote. */
    private readonly memo = new Map<string, { at: number; r: RegimeFornecedor }>();

    private get urlBase(): string {
        return process.env.SIMPLES_CONSULTA_URL_BASE || 'https://www.receitaws.com.br/v1/cnpj';
    }

    private get timeoutMs(): number {
        return Number(process.env.SIMPLES_CONSULTA_TIMEOUT_MS) || 20000;
    }

    private get memoTtlMs(): number {
        const v = Number(process.env.SIMPLES_MEMO_TTL_MS);
        return Number.isFinite(v) && v >= 0 ? v : 10 * 60 * 1000;
    }

    /** Consulta na ReceitaWS se o CNPJ é optante do Simples Nacional. */
    async consultarOptante(cnpj: string): Promise<RegimeFornecedor> {
        const doc = String(cnpj || '').replace(/\D/g, '');
        if (doc.length !== 14) {
            // CPF (produtor rural etc.) não tem consulta de Simples PJ
            return { optanteSimples: null, fonte: 'indefinido' };
        }

        const memoized = this.memo.get(doc);
        if (memoized && memoized.r.optanteSimples !== null && Date.now() - memoized.at < this.memoTtlMs) {
            return memoized.r;
        }

        const resultado = await this.consultar(doc);
        this.memo.set(doc, { at: Date.now(), r: resultado });
        return resultado;
    }

    private async consultar(doc: string): Promise<RegimeFornecedor> {
        try {
            const { status, json } = await this.getJson(`${this.urlBase}/${doc}`);
            if (status === 429) throw new Error('limite de requisições atingido (HTTP 429)');
            if (json?.status === 'ERROR') throw new Error(json.message || 'erro na consulta');

            const optSimples = !!(json?.simples && String(json.simples.optante).toLowerCase() === 'true');
            const optMei = !!(json?.simei && String(json.simei.optante).toLowerCase() === 'true');
            return {
                optanteSimples: optSimples || optMei,
                fonte: 'receitaws',
                razaoSocial: json?.nome ? String(json.nome) : undefined,
            };
        } catch (e) {
            this.logger.warn(`ReceitaWS falhou p/ CNPJ ${doc}: ${e instanceof Error ? e.message : e}`);
            return { optanteSimples: null, fonte: 'indefinido' };
        }
    }

    private getJson(url: string): Promise<{ status: number; json: any }> {
        return new Promise((resolve, reject) => {
            const req = httpsGet(url, { headers: { Accept: 'application/json' } }, (res) => {
                const chunks: Buffer[] = [];
                res.on('data', (c) => chunks.push(c as Buffer));
                res.on('end', () => {
                    const text = Buffer.concat(chunks).toString('utf-8');
                    try {
                        resolve({ status: res.statusCode || 0, json: text ? JSON.parse(text) : {} });
                    } catch {
                        reject(new Error(`Resposta não-JSON (HTTP ${res.statusCode}): ${text.slice(0, 120)}`));
                    }
                });
            });
            req.on('error', reject);
            req.setTimeout(this.timeoutMs, () => {
                req.destroy();
                reject(new Error(`Timeout (${this.timeoutMs}ms)`));
            });
        });
    }
}
