import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { IcmsService } from './icms.service';

/**
 * Loop fechado da auditoria fiscal via WhatsApp (WAHA), 100% de SAÍDA.
 *
 * Topologia: o WAHA/n8n rodam num EasyPanel ONLINE (Hostinger) e este serviço
 * roda no EasyPanel LOCAL (intranet). A intranet alcança a nuvem (saída HTTPS),
 * mas a nuvem NÃO alcança a intranet — então não dá para receber webhook do WAHA
 * aqui. Por isso fazemos POLLING: lemos as mensagens do grupo no WAHA, achamos as
 * respostas "ajustado" (citando o alerta de auditoria), reconferimos a NF e
 * respondemos no próprio grupo (✅ 100% ou ⚠️ erros restantes).
 *
 * Config por env:
 *  - WAHA_BASE_URL / WAHA_API_KEY / WAHA_SESSION / WAHA_GROUP_CHAT_ID (obrigatórios)
 *  - WAHA_AJUSTADO_CRON: expressão cron (default a cada 1 min).
 *  - WAHA_AJUSTADO_CRON_DISABLED=true: desliga o polling.
 */
@Injectable()
export class AuditoriaAjustadoCron {
  private readonly logger = new Logger(AuditoriaAjustadoCron.name);
  private rodando = false;

  constructor(private readonly icms: IcmsService) {}

  @Cron(process.env.WAHA_AJUSTADO_CRON || '* * * * *', {
    name: 'waha-auditoria-ajustado',
  })
  async poll() {
    if (process.env.WAHA_AJUSTADO_CRON_DISABLED === 'true') return;
    if (this.rodando) {
      this.logger.warn('Polling anterior de respostas "ajustado" ainda em execução; pulando.');
      return;
    }
    this.rodando = true;
    try {
      await this.icms.processarRespostasAjustadoWaha();
    } catch (err) {
      this.logger.error(
        `Falha no polling de respostas "ajustado": ${err instanceof Error ? err.message : String(err)}`,
      );
    } finally {
      this.rodando = false;
    }
  }
}
