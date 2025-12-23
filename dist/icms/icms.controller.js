"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
var __param = (this && this.__param) || function (paramIndex, decorator) {
    return function (target, key) { decorator(target, key, paramIndex); }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.IcmsController = void 0;
const common_1 = require("@nestjs/common");
const icms_service_1 = require("./icms.service");
let IcmsController = class IcmsController {
    constructor(service) {
        this.service = service;
    }
    async getInvoices(start, end) {
        return this.service.syncInvoices(start, end);
    }
    async calculate(body) {
        const results = [];
        for (const xml of body.xmls) {
            const itemResults = await this.service.calculateStForInvoice(xml);
            results.push(...itemResults);
        }
        return results;
    }
    async savePaymentStatus(body) {
        if (Array.isArray(body)) {
            const results = [];
            for (const item of body) {
                results.push(await this.service.savePaymentStatus(item));
            }
            return results;
        }
        return this.service.savePaymentStatus(body);
    }
    async getPaymentStatus() {
        return this.service.getPaymentStatusMap();
    }
    async generateDanfe(body, res) {
        const buffer = await this.service.generateDanfe(body.xml);
        res.set({
            'Content-Type': 'application/pdf',
            'Content-Disposition': 'inline; filename="danfe.pdf"',
        });
        return new common_1.StreamableFile(buffer);
    }
    async generateDanfeBatch(body, res) {
        const buffer = await this.service.generateDanfeZip(body.invoices);
        res.set({
            'Content-Type': 'application/zip',
            'Content-Disposition': 'attachment; filename="danfes.zip"',
        });
        return new common_1.StreamableFile(buffer);
    }
};
exports.IcmsController = IcmsController;
__decorate([
    (0, common_1.Get)('nfe-distribuicao'),
    __param(0, (0, common_1.Query)('start')),
    __param(1, (0, common_1.Query)('end')),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [String, String]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "getInvoices", null);
__decorate([
    (0, common_1.Post)('calculate'),
    __param(0, (0, common_1.Body)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "calculate", null);
__decorate([
    (0, common_1.Post)('payment-status'),
    __param(0, (0, common_1.Body)()),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "savePaymentStatus", null);
__decorate([
    (0, common_1.Get)('payment-status'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", []),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "getPaymentStatus", null);
__decorate([
    (0, common_1.Post)('danfe'),
    __param(0, (0, common_1.Body)()),
    __param(1, (0, common_1.Res)({ passthrough: true })),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object, Object]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "generateDanfe", null);
__decorate([
    (0, common_1.Post)('danfe/batch'),
    __param(0, (0, common_1.Body)()),
    __param(1, (0, common_1.Res)({ passthrough: true })),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object, Object]),
    __metadata("design:returntype", Promise)
], IcmsController.prototype, "generateDanfeBatch", null);
exports.IcmsController = IcmsController = __decorate([
    (0, common_1.Controller)('icms'),
    __metadata("design:paramtypes", [icms_service_1.IcmsService])
], IcmsController);
//# sourceMappingURL=icms.controller.js.map