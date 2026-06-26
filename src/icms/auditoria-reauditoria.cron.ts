import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { IcmsService } from './icms.service';

/**
 * Reauditoria periódica das NF LANCADA ainda NÃO alertadas (janela curta).
 *
 * O alerta automático de auditoria (WhatsApp) só dispara no exato momento em que
 * a NF vira LANCADA. Quando a divergência aparece DEPOIS — típico de uso/consumo,
 * em que o cadastro do produto é ajustado após a entrada — a nota fica DIVERGENTE
 * sem nunca alertar. Este cron fecha a lacuna: reaudita as LANCADA não-alertadas
 * dos últimos N dias e, havendo erro, envia (idempotente: 1x por nota, via guarda
 * auditoria_alerta_em em auditarLancamentoFiscal).
 *
 * Config por env:
 *  - AUDITORIA_REAUDIT_CRON: expressão cron (default a cada 15 min).
 *  - AUDITORIA_REAUDIT_DIAS: janela de dias para trás por dt_entrada (default 7).
 *  - AUDITORIA_REAUDIT_LIMITE: teto de notas por ciclo (default 300).
 *  - AUDITORIA_REAUDIT_CRON_DISABLED=true: desliga.
 */
@Injectable()
export class AuditoriaReauditoriaCron {
  private readonly logger = new Logger(AuditoriaReauditoriaCron.name);
  private rodando = false;

  constructor(private readonly icms: IcmsService) {}

  @Cron(process.env.AUDITORIA_REAUDIT_CRON || '*/15 * * * *', {
    name: 'auditoria-reauditoria',
  })
  async run() {
    if (process.env.AUDITORIA_REAUDIT_CRON_DISABLED === 'true') return;
    if (this.rodando) {
      this.logger.warn('Reauditoria anterior ainda em execução; pulando este disparo.');
      return;
    }
    this.rodando = true;
    try {
      const dias = this.lerNum(process.env.AUDITORIA_REAUDIT_DIAS, 7);
      const limite = this.lerNum(process.env.AUDITORIA_REAUDIT_LIMITE, 300);
      const { avaliadas } = await this.icms.reauditarPendentesAlerta(dias, limite);
      if (avaliadas > 0) {
        this.logger.log(`Reauditoria de pendentes de alerta concluída: ${avaliadas} nota(s) avaliada(s).`);
      }
    } catch (err) {
      this.logger.error(
        `Falha na reauditoria de pendentes de alerta: ${err instanceof Error ? err.message : String(err)}`,
      );
    } finally {
      this.rodando = false;
    }
  }

  private lerNum(raw: string | undefined, def: number): number {
    const n = Number(raw);
    return Number.isFinite(n) && n > 0 ? Math.floor(n) : def;
  }
}
