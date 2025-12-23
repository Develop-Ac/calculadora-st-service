-- CreateTable
CREATE TABLE "paid_invoices" (
    "chave_nfe" TEXT NOT NULL PRIMARY KEY,
    "payment_date" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "amount" REAL NOT NULL DEFAULT 0.0,
    "notes" TEXT NOT NULL DEFAULT ''
);
