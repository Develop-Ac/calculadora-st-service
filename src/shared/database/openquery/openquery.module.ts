import { Module, Global } from '@nestjs/common';
import { OpenQueryService } from './openquery.service';

@Global()
@Module({
    providers: [OpenQueryService],
    exports: [OpenQueryService],
})
export class OpenQueryModule { }
