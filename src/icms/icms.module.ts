import { Module } from '@nestjs/common';
import { IcmsController } from './icms.controller';
import { IcmsService } from './icms.service';
import { IcmsSyncCron } from './icms-sync.cron';
import { AuditoriaAjustadoCron } from './auditoria-ajustado.cron';

@Module({
    controllers: [IcmsController],
    providers: [IcmsService, IcmsSyncCron, AuditoriaAjustadoCron],
})
export class IcmsModule { }
