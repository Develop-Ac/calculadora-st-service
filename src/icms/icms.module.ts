import { Module } from '@nestjs/common';
import { IcmsController } from './icms.controller';
import { IcmsService } from './icms.service';

@Module({
    controllers: [IcmsController],
    providers: [IcmsService],
})
export class IcmsModule { }
