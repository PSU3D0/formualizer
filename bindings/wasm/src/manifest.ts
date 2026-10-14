export type FioValueType = 'string' | 'number' | 'integer' | 'boolean' | 'date' | 'datetime';
export interface FioConstraints { min?: number | null; max?: number | null; enum?: unknown[] | null; pattern?: string | null; nullable?: boolean | null }
export interface FioUnits { currency?: string | null }
export type FioFieldSelector = { a1: string } | { name: string } | { struct_ref: string };
export type FioSelector = FioFieldSelector | { table: { name: string; area?: 'header' | 'body' | 'totals' | null } } | { layout: { kind?: 'header_contiguous_v1'; sheet: string; header_row: number; anchor_col: string; terminate: 'first_blank_row' | 'sheet_end' | 'until_marker'; marker_text?: string | null } };
export interface FioColumn { name: string; type: FioValueType; col?: string | null; format?: string | null; units?: FioUnits | null }
export interface FioField { type: FioValueType; location: FioFieldSelector; constraints?: FioConstraints | null; units?: FioUnits | null }
export type FioSchema = { type: FioValueType; format?: string | null } | { kind?: 'range'; cell_type: FioValueType; format?: string | null } | { kind?: 'table'; columns: FioColumn[]; keys?: string[] | null } | { kind?: 'record'; fields: Record<string, FioField> };
export interface FioPort { id: string; dir: 'in' | 'out'; shape: 'scalar' | 'range' | 'table' | 'record'; location: FioSelector; schema: FioSchema; description?: string | null; required?: boolean; default?: unknown; constraints?: FioConstraints | null; units?: FioUnits | null; partition_key?: boolean | null }
export interface FioWorkbook { uri?: string | null; locale?: string | null; date_system?: number | null; timezone?: string | null }
export interface FioManifest { spec: 'fio'; spec_version: string; capabilities?: { profile?: 'core-v0' | 'full-v0'; features?: string[] | null } | null; manifest: { id: string; name: string; description?: string | null; tags?: string[] | null; workbook?: FioWorkbook | null; metadata?: Record<string, unknown> | null }; ports: FioPort[] }
export type FioPortInput = Omit<FioPort, 'location' | 'schema' | 'shape'> & { location: string | FioSelector; shape?: FioPort['shape'] } & ({ type: FioValueType; schema?: never } | { schema: FioSchema; type?: never });
export interface FioManifestInput { id: string; name?: string; description?: string; workbook?: FioWorkbook; tags?: string[]; ports: (FioPortInput | FioPort)[] }
export function defineManifest(input: FioManifestInput): FioManifest {
  const { id, name = id, ports, ...meta } = input;
  return { spec: 'fio', spec_version: '0.3.0', manifest: { id, name, ...meta }, ports: ports.map(port => {
    const { type, ...rest } = port as FioPortInput;
    const schema: FioSchema = rest.schema ?? { type: type! };
    const shape = 'cell_type' in schema ? 'range' : 'columns' in schema ? 'table' : 'fields' in schema ? 'record' : 'scalar';
    return { ...rest, schema, shape: rest.shape ?? shape, location: typeof rest.location === 'string' ? { a1: rest.location } : rest.location };
  }) };
}
