import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { IcmsService } from './icms.service';

/**
 * Sincronização PERIÓDICA das NF-e do ERP.
 *
 * Hoje o syncInvoices só roda quando a tela de NF-e é aberta ou quando o usuário
 * clica em "Atualizar". Como a NF chega primeiro como RESUMO (cabeçalho da SEFAZ)
 * e só após a manifestação o XML COMPLETO é baixado pelo ERP, sem um disparo
 * periódico a conciliação local (nfeConciliacao) fica defasada e o auto-vínculo
 * do compras-service não encontra os itens p/ casar com os pedidos.
 *
 * Este cron mantém o nfeConciliacao atualizado (captura a transição resumo→completo),
 * para que a varredura de auto-vínculo do compras-service consiga sugerir os
 * vínculos automaticamente.
 *
 * Config por env:
 *  - NFE_SYNC_CRON: expressão cron (default a cada 1 min).
 *  - NFE_SYNC_DIAS: janela de dias para trás (default 30).
 *  - NFE_SYNC_CRON_DISABLED=true: desliga o disparo periódico.
 */
@Injectable()
export class IcmsSyncCron {
  private readonly logger = new Logger(IcmsSyncCron.name);
  private rodando = false;

  constructor(private readonly icms: IcmsService) {}

  @Cron(process.env.NFE_SYNC_CRON || '* * * * *', {
    name: 'nfe-sync',
  })
  async sync() {
    if (process.env.NFE_SYNC_CRON_DISABLED === 'true') return;
    if (this.rodando) {
      this.logger.warn('Sync periódico anterior ainda em execução; pulando este disparo.');
      return;
    }

    this.rodando = true;
    try {
      const dias = this.lerDias();
      const { start, end } = this.janela(dias);
      this.logger.log(`Sync periódico de NF-e (${start} a ${end})...`);
      const lista = await this.icms.syncInvoices(start, end);
      const total = Array.isArray(lista) ? lista.length : 0;
      this.logger.log(`Sync periódico de NF-e concluído: ${total} nota(s) na janela.`);
    } catch (err) {
      this.logger.error(
        `Falha no sync periódico de NF-e: ${err instanceof Error ? err.message : String(err)}`,
      );
    } finally {
      this.rodando = false;
    }
  }

  private lerDias(): number {
    const raw = Number(process.env.NFE_SYNC_DIAS);
    return Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : 30;
  }

  private janela(dias: number): { start: string; end: string } {
    const fmt = (d: Date) =>
      `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
    const end = new Date();
    const start = new Date(end.getTime() - dias * 24 * 60 * 60 * 1000);
    return { start: fmt(start), end: fmt(end) };
  }
}
