// src/api/client.ts
import axios from "axios";

const BASE = "http://localhost:8000";

export interface MigrationStep {
  step_id: number;
  description: string;
  status: string;
}

export interface ValidationResult {
  original_row_count: number;
  rewritten_row_count: number;
  checksum_match: boolean;
  passed: boolean;
  diff_sample: unknown[] | null;
}

export interface MigrationResult {
  intent: Record<string, unknown>;
  migration_plan: MigrationStep[];
  legacy_sql: string;
  transpiled_sql: string | null;
  rewritten_sql: string | null;
  validation: ValidationResult | null;
  retry_count: number;
  success: boolean;
  error: string | null;
}

export async function runMigration(
  file: File,
  targetDialect: string,
  sourceDialect: string
): Promise<MigrationResult> {
  const form = new FormData();
  form.append("file", file);
  form.append("target_dialect", targetDialect);
  form.append("source_dialect", sourceDialect);
  const { data } = await axios.post<MigrationResult>(`${BASE}/migrate`, form, {
    headers: { "Content-Type": "multipart/form-data" },
  });
  return data;
}

export async function checkHealth(): Promise<boolean> {
  try {
    await axios.get(`${BASE}/health`);
    return true;
  } catch {
    return false;
  }
}
