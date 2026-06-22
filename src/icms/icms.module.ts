import { Module } from '@nestjs/common';
import { IcmsController } from './icms.controller';
import { IcmsService } from './icms.service';
import { IcmsSyncCron } from './icms-sync.cron';

@Module({
    controllers: [IcmsController],
    providers: [IcmsService, IcmsSyncCron],
})
export class IcmsModule { }
