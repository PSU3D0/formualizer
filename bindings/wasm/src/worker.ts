import type {
  WorkbookApi, WorkbookLoadOptions, CellValue, CellAddress, FiniteRangeAddress, RangeArea,
  CellSnapshot, CustomFunctionOptions, RegisteredFunctionInfo, SheetPortEvaluateOptions,
  SheetPortSessionApi, XlsxBytesSource, XlsxRecalculateOptions, XlsxRecalculateResult, FormulaDialect,
  Token, ASTNodeData,
} from './index.js';
import type { RecalcJob, RecalcJobOptions, RecalcResult, RecalcProgress, StepReport } from './recalc.js';
import type { FioManifest } from './manifest.js';

/** Both DOM and node:worker_threads endpoints are accepted without a runtime dependency. */
export interface WorkerEndpoint {
  postMessage(message: any, transfer?: any[]): void;
  addEventListener?: Function;
  removeEventListener?: Function;
  on?: Function;
  off?: Function;
  removeListener?: Function;
  start?: () => void;
  close?: () => void;
  terminate?: () => unknown;
}
export type WorkerRecalcStatus = 'idle' | 'calculating' | 'paused' | 'cancelled' | 'error';
export interface WorkerStateHeader {
  revision: number;
  evaluatedRevision: number;
  ackClientSeq: number;
  recalcStatus: WorkerRecalcStatus;
  hasActiveJob: boolean;
}
export interface RequestOptions { signal?: AbortSignal }
export interface TransferRequestOptions extends RequestOptions { transfer?: boolean }
export interface ReadResult<T> extends WorkerStateHeader { data: T }
export type MutationResult = WorkerStateHeader;
export type WorkerRecalcOutcome = WorkerStateHeader & (
  { status: 'Finished'; result: RecalcResult } | { status: 'Paused' | 'Cancelled' }
);
export interface WorkerRecalcOptions extends RecalcJobOptions, RequestOptions {
  budgetMs?: number | null;
  onProgress?: (report: StepReport & WorkerStateHeader) => unknown | PromiseLike<unknown>;
  onStep?: (report: StepReport & WorkerStateHeader) => void | boolean;
  pauseOnReturn?: boolean;
}
export interface CreateWorkbookWorkerOptions {
  source?: { kind: 'empty'; options?: WorkbookLoadOptions } |
    { kind: 'json'; json: string; options?: WorkbookLoadOptions } |
    { kind: 'xlsx'; bytes: XlsxBytesSource; options?: WorkbookLoadOptions; transfer?: boolean };
  autoRecalc?: boolean;
  autoRecalcBudgetMs?: number;
  maxDeltaRecords?: number;
  terminateOnDispose?: boolean;
}
type Callback = (...args: CellValue[]) => CellValue;
export type WorkerFunctions = Record<string, Callback | { callback: Callback; options?: CustomFunctionOptions }>;
export interface AttachWorkbookWorkerOptions {
  runtime?: typeof import('./index.js');
  functions?: WorkerFunctions;
  allowFunctionFromSource?: boolean;
  clearFunctionsOnLoad?: boolean;
  importModule?: (url: string) => Promise<Record<string, unknown>>;
}
export type InProcessWorkbookWorkerOptions = CreateWorkbookWorkerOptions & AttachWorkbookWorkerOptions;
export type WorkbookEditOp =
  | { kind: 'setValue'; sheet: string; row: number; column: number; value: CellValue }
  | { kind: 'setFormula'; sheet: string; row: number; column: number; formula: string }
  | { op: 'setValue'; sheet: string; row: number; col: number; value: CellValue }
  | { op: 'setFormula'; sheet: string; row: number; col: number; formula: string }
  | { op: 'setValues'; sheet: string; startRow: number; startCol: number; values: CellValue[][] }
  | { op: 'setFormulas'; sheet: string; startRow: number; startCol: number; formulas: string[][] }
  | { op: 'addSheet'; name: string };
export type ViewportDataMode = 'values' | 'snapshots' | 'deltas' | 'columnar';
export interface ViewportCellPatch { row: number; column: number; value: CellValue }
export interface ViewportColumnarBuffer {
  rect: FiniteRangeAddress; rows: number; cols: number;
  /** 0 empty, 1 number, 2 boolean, 3 string, 4 error, 5 temporal/other JSON. */
  tags: Uint8Array; numbers: Float64Array; strings: string[]; pendingBitset: Uint8Array;
}
interface ViewportFrameBase extends WorkerStateHeader {
  subscriptionId: string; frameSeq: number; rect: FiniteRangeAddress;
  hasInFlightMutations: boolean;
  reason: 'initial' | 'mutation' | 'step' | 'finished' | 'rect-change' | 'manual';
  phase: StepReport['phase'] | 'Idle'; progress: RecalcProgress | null;
  changedRects: FiniteRangeAddress[]; fullRefresh: boolean;
  hasPending: boolean; pendingCells?: CellAddress[];
}
export type ViewportFrame = ViewportFrameBase & (
  { mode: 'values'; grid?: CellValue[][]; patches?: ViewportCellPatch[] } |
  { mode: 'snapshots'; snapshots: CellSnapshot[] } |
  { mode: 'deltas' } | { mode: 'columnar'; columnar: ViewportColumnarBuffer }
);
export interface ViewportSubscriptionOptions {
  rect: FiniteRangeAddress; mode?: ViewportDataMode; prioritize?: boolean;
  immediate?: boolean; throttleMs?: number; includePendingCells?: boolean;
  onFrame: (frame: ViewportFrame) => void;
  onError?: (error: Error) => void;
}
export interface ViewportSubscription {
  readonly id: string; readonly rect: FiniteRangeAddress;
  setRect(rect: FiniteRangeAddress): Promise<void>;
  refresh(): Promise<ViewportFrame>;
  unsubscribe(): Promise<void>;
  [Symbol.dispose](): void;
}
export interface WorkerSheetPortSession {
  manifest(req?: RequestOptions): Promise<FioManifest>;
  describePorts(req?: RequestOptions): Promise<unknown>;
  readInputs(req?: RequestOptions): Promise<Record<string, unknown>>;
  readOutputs(req?: RequestOptions): Promise<Record<string, unknown>>;
  writeInputs(inputs: Record<string, unknown>, req?: RequestOptions): Promise<WorkerStateHeader>;
  evaluateOnce(options?: SheetPortEvaluateOptions, req?: RequestOptions): Promise<Record<string, unknown>>;
  dispose(): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>;
}

const errorKeys = ['kind','code','inspect_code','explicit','workbook_kind','excel_kind','excel_message',
  'resource_reason','limit','observed','request_id','function','panic_message','extra',
  'preparation_stale_reason','plan_stale_reason','reason','context','pos'];
export interface SerializedWorkerError {
  name: string; message: string; stack?: string; cause?: SerializedWorkerError;
  [key: string]: unknown;
}
/** Preserve native non-enumerable domain fields as well as arbitrary custom properties. */
export function serializeWorkerError(error: unknown, seen = new Set<unknown>()): SerializedWorkerError {
  if (seen.has(error)) return { name: 'Error', message: '[circular cause]' };
  seen.add(error);
  const e = error as Record<string, any> | null;
  const result: SerializedWorkerError = {name: e?.name ?? 'Error', message: e?.message ?? String(error)};
  if(e?.stack) result.stack=e.stack;
  if(e && typeof e === 'object') {
    for(const key of new Set([...Object.keys(e), ...errorKeys])) {
      if(!['name','message','stack','cause'].includes(key) && key in e) {
        try { result[key]=structuredClone(e[key]); } catch { result[key]=String(e[key]); }
      }
    }
    if(e.cause !== undefined) result.cause=serializeWorkerError(e.cause,seen);
  }
  return result;
}
export function deserializeWorkerError(value: SerializedWorkerError): Error {
  const error = value.name === 'AbortError' ? new DOMException(value.message,'AbortError') : new Error(value.message);
  for(const [key,val] of Object.entries(value)) {
    if(key==='cause') Object.defineProperty(error,key,{value:deserializeWorkerError(val as SerializedWorkerError),configurable:true});
    else { try { Object.defineProperty(error,key,{value:val,writable:true,configurable:true,enumerable:!['name','message','stack'].includes(key)}); } catch {} }
  }
  return error;
}
export function prepareTransferableBytes(source: XlsxBytesSource, transfer = false): { bytes: Uint8Array; transfer: ArrayBuffer[] } {
  let bytes = source instanceof Uint8Array ? source : new Uint8Array(source);
  if(transfer && (bytes.byteOffset !== 0 || bytes.byteLength !== bytes.buffer.byteLength || !(bytes.buffer instanceof ArrayBuffer) || bytes.constructor !== Uint8Array)) bytes=new Uint8Array(bytes);
  return {bytes,transfer:transfer ? [bytes.buffer as ArrayBuffer] : []};
}
function domain(code: string, message = code, extra = {}): Error {
  return Object.assign(new Error(message),{name:'RecalcJobError',kind:'RecalcJobError',code,...extra});
}
function abortError(): Error { return new DOMException('Request aborted','AbortError'); }
function listen(endpoint: WorkerEndpoint, receive: (value: any) => void, failure: (error: any) => void): () => void {
  const listeners: [string, Function][]=[];
  const add=(event:string, fn:Function)=>{
    listeners.push([event,fn]);
    if(endpoint.addEventListener) endpoint.addEventListener(event,fn);
    else endpoint.on?.(event,fn);
  };
  add('message',endpoint.addEventListener ? (event:any)=>receive(event.data) : receive);
  add('messageerror',failure); add('error',failure);
  if(!endpoint.addEventListener) add('exit',(code:number)=>failure(new Error('Worker exited: '+code)));
  endpoint.start?.();
  return ()=>{for(const [event,fn] of listeners) {
    if(endpoint.removeEventListener) endpoint.removeEventListener(event,fn);
    else (endpoint.off ?? endpoint.removeListener)?.call(endpoint,event,fn);
  }};
}
const initialState = (): WorkerStateHeader => ({revision:0,evaluatedRevision:0,ackClientSeq:0,recalcStatus:'idle',hasActiveJob:false});
const mutationMethods=new Set(['loadEmpty','loadFromJson','loadFromXlsxBytes','addSheet','addTable','setTemporalEgress',
  'setValue','setFormula','setValues','setFormulas','editBatch','beginAction','endAction','undo','redo',
  'registerFunctionModule','registerFunctionFromSource','unregisterFunction','session:writeInputs','evaluateSheetPort']);
const workbookMethods=['addSheet','sheetNames','addTable','getTables','getNamedRanges','setTemporalEgress','setChangelogEnabled',
  'beginAction','endAction','undo','redo','inspectCell','precedents','dependents','trace','rangePage','getEvalPlan',
  'lastCycleTelemetry','evaluateAll','evaluateCell','evaluateCells','hasPendingInRect','pendingCellsInRect','listFunctions','unregisterFunction'] as const;
type WorkbookRpc = {
  [K in typeof workbookMethods[number]]: WorkbookApi[K] extends (...args: infer A) => infer R ?
    (...args: [...A, req?: RequestOptions]) => Promise<K extends 'addSheet'|'addTable'|'setTemporalEgress'|'beginAction'|'endAction'|'undo'|'redo'|'unregisterFunction' ? WorkerStateHeader : R> : never
};
export interface WorkbookWorkerClient extends WorkbookRpc {}
export class WorkbookWorkerClient {
  private current=initialState();
  private failure:unknown;
  private seq=0; private next=0; private disposed=false;
  private pending=new Map<number,{resolve:(v:any)=>void;reject:(e:unknown)=>void;cleanup:()=>void}>();
  private listeners=new Set<(state:WorkerStateHeader)=>void>();
  private viewports=new Map<string,ViewportSubscriptionOptions>();
  private unlisten:()=>void;
  private isReady=false;
  private ready:Promise<void>;
  private resolveReady!:()=>void;
  private rejectReady!:(error:unknown)=>void;
  constructor(private endpoint:WorkerEndpoint, private options:CreateWorkbookWorkerOptions={}) {
    this.ready=new Promise((resolve,reject)=>{this.resolveReady=resolve;this.rejectReady=reject;});
    this.unlisten=listen(endpoint,m=>this.receive(m),e=>this.fail(e instanceof Error?e:new Error(e?.message??String(e))));
    endpoint.postMessage({protocol:'formualizer-worker',type:'hello'});
  }
  get state(): WorkerStateHeader { return {...this.current}; }
  get lastSentClientSeq(): number { return this.seq; }
  get hasInFlightMutations(): boolean { return this.current.ackClientSeq<this.seq; }
  onStatusChange(listener:(state:WorkerStateHeader)=>void):()=>void {this.listeners.add(listener);return ()=>{this.listeners.delete(listener);};}
  private fail(error:unknown) {
    this.failure=error;
    this.rejectReady(error);
    for(const p of this.pending.values()){p.cleanup();p.reject(error);} this.pending.clear();
  }
  private receive(m:any) {
    if(m?.protocol!=='formualizer-worker')return;
    if(m.state) {
      this.current=m.state;
      for(const listener of this.listeners) {try {listener(this.state);} catch {}}
    }
    if(m.type==='ready'){this.isReady=true;this.resolveReady();}
    if(m.type==='viewport') {
      const sub=this.viewports.get(m.id);
      try { if(m.error) sub?.onError?.(deserializeWorkerError(m.error)); else sub?.onFrame({...m.frame,hasInFlightMutations:m.frame.ackClientSeq<this.seq}); }
      catch(error){try{sub?.onError?.(error as Error);}catch{}}
      finally {this.endpoint.postMessage({protocol:'formualizer-worker',type:'viewport:ack',id:m.id,frameSeq:m.frame?.frameSeq});}
    } else if(m.type==='reply') {
      const p=this.pending.get(m.id); if(!p)return; this.pending.delete(m.id);p.cleanup();
      if(m.error)p.reject(deserializeWorkerError(m.error));else p.resolve(m.data);
    }
  }
  /** Internal RPC; callers use the typed methods below. */
  private request(method:string,args:unknown[]=[],req?:RequestOptions,transfer:any[]=[]):Promise<any> {
    // Browser module workers can yield during WASM initialization before installing listeners.
    // Do not send the first command until the host announces readiness.
    return this.isReady?this.sendRequest(method,args,req,transfer):this.ready.then(()=>this.sendRequest(method,args,req,transfer));
  }
  private sendRequest(method:string,args:unknown[]=[],req?:RequestOptions,transfer:any[]=[]):Promise<any> {
    if(this.disposed)return Promise.reject(new Error('Workbook worker disposed'));
    if(this.failure!==undefined)return Promise.reject(this.failure);
    if(req?.signal?.aborted)return Promise.reject(abortError());
    const id=++this.next;
    const clientSeq=mutationMethods.has(method)?++this.seq:undefined;
    return new Promise((resolve,reject)=>{
      const abort=()=>{this.pending.delete(id);req?.signal?.removeEventListener('abort',abort);reject(abortError());};
      const cleanup=()=>req?.signal?.removeEventListener('abort',abort);
      this.pending.set(id,{resolve,reject,cleanup});req?.signal?.addEventListener('abort',abort,{once:true});
      try{this.endpoint.postMessage({protocol:'formualizer-worker',type:'request',id,method,args,clientSeq},transfer);}
      catch(e){this.pending.delete(id);cleanup();reject(e);}
    });
  }
  loadEmpty(options?:WorkbookLoadOptions,req?:RequestOptions):Promise<WorkerStateHeader>{return this.request('loadEmpty',[options],req);}
  loadFromJson(json:string,options?:WorkbookLoadOptions,req?:RequestOptions):Promise<WorkerStateHeader>{return this.request('loadFromJson',[json,options],req);}
  loadFromXlsxBytes(bytes:XlsxBytesSource,options?:WorkbookLoadOptions,req?:TransferRequestOptions):Promise<WorkerStateHeader>{
    const input=prepareTransferableBytes(bytes,req?.transfer);return this.request('loadFromXlsxBytes',[input.bytes,options],req,input.transfer);
  }
  setAutoRecalc(enabled:boolean,options?:{budgetMs?:number;maxDeltaRecords?:number}):Promise<WorkerStateHeader>{return this.request('setAutoRecalc',[enabled,options]);}
  setValue(sheet:string,row:number,col:number,value:CellValue):Promise<WorkerStateHeader>{return this.request('setValue',[sheet,row,col,value]);}
  setFormula(sheet:string,row:number,col:number,formula:string):Promise<WorkerStateHeader>{return this.request('setFormula',[sheet,row,col,formula]);}
  setValues(sheet:string,row:number,col:number,values:CellValue[][]):Promise<WorkerStateHeader>{return this.request('setValues',[sheet,row,col,values]);}
  setFormulas(sheet:string,row:number,col:number,formulas:string[][]):Promise<WorkerStateHeader>{return this.request('setFormulas',[sheet,row,col,formulas]);}
  editBatch(ops:WorkbookEditOp[],options?:{actionDescription?:string}):Promise<WorkerStateHeader>{return this.request('editBatch',[ops,options]);}
  getValue(sheet:string,row:number,col:number,req?:RequestOptions):Promise<CellValue>{return this.request('getValue',[sheet,row,col],req);}
  getFormula(sheet:string,row:number,col:number,req?:RequestOptions):Promise<string|undefined>{return this.request('getFormula',[sheet,row,col],req);}
  getValueWithState(sheet:string,row:number,col:number,req?:RequestOptions):Promise<ReadResult<CellValue>>{return this.request('getValueWithState',[sheet,row,col],req);}
  readRange(sheet:string,startRow:number,startCol:number,endRow:number,endCol:number,req?:RequestOptions):Promise<CellValue[][]>{return this.request('readRange',[sheet,startRow,startCol,endRow,endCol],req);}
  readRangeWithState(area:FiniteRangeAddress,req?:RequestOptions):Promise<ReadResult<CellValue[][]>>{return this.request('readRangeWithState',[area],req);}
  beginRecalc(options?:RecalcJobOptions):Promise<WorkerStateHeader>{return this.request('beginRecalc',[options]);}
  stepRecalc(budgetMs?:number|null):Promise<StepReport & WorkerStateHeader>{return this.request('stepRecalc',[budgetMs]);}
  pauseRecalc():Promise<WorkerStateHeader>{return this.request('pauseRecalc');}
  cancelRecalc():Promise<(StepReport & WorkerStateHeader)|null>{return this.request('cancelRecalc');}
  prioritize(ranges:FiniteRangeAddress[]):Promise<WorkerStateHeader>{return this.request('prioritize',[ranges]);}
  async recalc(options:WorkerRecalcOptions={}):Promise<WorkerRecalcOutcome>{
    let last:StepReport & WorkerStateHeader|undefined;
    const stream=this.recalcStream({...options,pauseOnReturn:true});
    for await(const report of stream){last=report;
      await abortable(()=>options.onProgress?.(report),options.signal);
      if(options.onStep?.(report)===false && report.phase!=='Finished' && report.phase!=='Cancelled') {await stream.return?.();return {status:'Paused',...this.state};}}
    if(last?.phase==='Finished')return {status:'Finished',result:last.result,...this.state};
    return {status:this.state.recalcStatus==='paused'?'Paused':'Cancelled',...this.state};
  }
  async *recalcStream(options:WorkerRecalcOptions={}):AsyncGenerator<StepReport & WorkerStateHeader,void,unknown>{
    if(options.signal?.aborted)throw abortError();
    validateBudget(options.budgetMs);
    let driverId:number|undefined;
    const abort=()=>{if(driverId!==undefined)void this.request('drive:abort',[driverId]).catch(()=>{});};
    options.signal?.addEventListener('abort',abort,{once:true});
    try {
      const admission=await this.request('drive:start',[jobOptions(options)]);driverId=admission.driverId;
      if(options.signal?.aborted){await this.request('drive:abort',[driverId]);throw abortError();}
      for(;;){
        const report=await this.request('drive:next',[driverId,options.budgetMs]);
        if(options.signal?.aborted)throw abortError();
        if(!report)break;
        yield report;
        if(report.phase==='Finished'||report.phase==='Cancelled')break;
      }
    } finally {
      options.signal?.removeEventListener('abort',abort);
      if(driverId!==undefined)await this.request('drive:end',[driverId,options.pauseOnReturn!==false]).catch(()=>{});
    }
  }
  async createSheetPortSession(manifest:FioManifest|string,req?:RequestOptions):Promise<WorkerSheetPortSession>{
    const id=await this.request('session:create',[manifest],req);
    const call=(method:string,args:unknown[]=[],req?:RequestOptions)=>this.request('session:'+method,[id,...args],req);
    const session:WorkerSheetPortSession={
      manifest:req=>call('manifest',[],req),describePorts:req=>call('describePorts',[],req),
      readInputs:req=>call('readInputs',[],req),readOutputs:req=>call('readOutputs',[],req),
      writeInputs:(inputs,req)=>call('writeInputs',[inputs],req),
      evaluateOnce:(options,req)=>call('evaluateOnce',[options],req),dispose:()=>call('dispose'),
      [Symbol.asyncDispose]:()=>call('dispose'),
    };return session;
  }
  evaluateSheetPort(manifest:FioManifest|string,options?:{inputs?:Record<string,unknown>;evaluateOptions?:SheetPortEvaluateOptions},req?:RequestOptions):Promise<Record<string,unknown>>{
    return this.request('evaluateSheetPort',[manifest,options],req);
  }
  recalculateXlsxBytes(bytes:XlsxBytesSource,errorLocationLimit?:number,options?:XlsxRecalculateOptions,req?:TransferRequestOptions):Promise<XlsxRecalculateResult>{
    const input=prepareTransferableBytes(bytes,req?.transfer);return this.request('recalculateXlsxBytes',[input.bytes,errorLocationLimit,options,req?.transfer],req,input.transfer);
  }
  tokenize(formula:string,dialect?:FormulaDialect,req?:RequestOptions):Promise<Token[]>{return this.request('tokenize',[formula,dialect],req);}
  parse(formula:string,dialect?:FormulaDialect,req?:RequestOptions):Promise<ASTNodeData>{return this.request('parse',[formula,dialect],req);}
  registerFunctionModule(url:string,options?:{exportName?:string}):Promise<RegisteredFunctionInfo[]>{return this.request('registerFunctionModule',[url,options]);}
  registerFunctionFromSource(name:string,source:string,options?:CustomFunctionOptions):Promise<RegisteredFunctionInfo>{return this.request('registerFunctionFromSource',[name,source,options]);}
  async subscribeViewport(options:ViewportSubscriptionOptions):Promise<ViewportSubscription>{
    validateRect(options.rect);
    const id='v'+(++this.next);this.viewports.set(id,options);
    const {onFrame,onError,...wire}=options;
    try{await this.request('viewport:subscribe',[id,wire]);}catch(error){this.viewports.delete(id);throw error;}
    let rect=options.rect;
    return {id,get rect(){return rect;},
      setRect:async value=>{validateRect(value);await this.request('viewport:rect',[id,value]);rect=value;},
      refresh:()=>this.request('viewport:refresh',[id]),
      unsubscribe:async()=>{this.viewports.delete(id);await this.request('viewport:unsubscribe',[id]);},
      [Symbol.dispose]:()=>{this.viewports.delete(id);void this.request('viewport:unsubscribe',[id]).catch(()=>{});},
    };
  }
  async dispose():Promise<void>{
    if(this.disposed)return;
    try{await this.request('dispose');}finally{
      this.disposed=true;this.unlisten();this.fail(new Error('Workbook worker disposed'));this.viewports.clear();this.listeners.clear();
      this.endpoint.close?.();if(this.options.terminateOnDispose!==false)await this.endpoint.terminate?.();
    }
  }
  [Symbol.asyncDispose]():Promise<void>{return this.dispose();}
}
for(const method of workbookMethods) Object.defineProperty(WorkbookWorkerClient.prototype,method,{
  value:function(this:WorkbookWorkerClient,...args:unknown[]){
    let req:RequestOptions|undefined;
    const last=args.at(-1);
    if(last && typeof last==='object' && 'signal' in last)req=args.pop() as RequestOptions;
    return (this as any).request(method,args,req);
  }
});
export async function createWorkbookWorker(endpoint:WorkerEndpoint,options:CreateWorkbookWorkerOptions={}):Promise<WorkbookWorkerClient>{
  const client=new WorkbookWorkerClient(endpoint,options);
  try {
    await (client as any).request('initialize',[options.autoRecalc??false,options.autoRecalcBudgetMs,options.maxDeltaRecords]);
    const source=options.source;
    if(source?.kind==='json')await client.loadFromJson(source.json,source.options);
    else if(source?.kind==='xlsx')await client.loadFromXlsxBytes(source.bytes,source.options,{transfer:source.transfer});
    else if(source)await client.loadEmpty(source.options);
    return client;
  }catch(error){await client.dispose().catch(()=>{});throw error;}
}
export async function createInProcessWorkbookWorker(options:InProcessWorkbookWorkerOptions={}):Promise<WorkbookWorkerClient>{
  const channel=new MessageChannel();
  await attachWorkbookWorker(channel.port1,options);
  return createWorkbookWorker(channel.port2,options);
}
function abortable(hook:()=>unknown,signal?:AbortSignal):Promise<unknown>{
  if(signal?.aborted)return Promise.reject(abortError());
  return new Promise((resolve,reject)=>{
    const abort=()=>{signal?.removeEventListener('abort',abort);reject(abortError());};
    signal?.addEventListener('abort',abort,{once:true});
    Promise.resolve().then(()=>{if(signal?.aborted)throw abortError();return hook();})
      .then(resolve,reject).finally(()=>signal?.removeEventListener('abort',abort));
  });
}
function validateBudget(value:unknown){
  if(value!==undefined && value!==null && (typeof value!=='number'||!Number.isFinite(value)||value<0||value/1000>=2**64))throw domain('INVALID_OPTIONS','Invalid budgetMs');
}
function validateRect(r:FiniteRangeAddress){
  if(!r || typeof r.sheet!=='string'||!r.sheet || ![r.startRow,r.startColumn,r.endRow,r.endColumn].every(v=>Number.isSafeInteger(v)&&v>0) ||
    r.startRow>r.endRow||r.startColumn>r.endColumn||r.endRow>1048576||r.endColumn>16384)throw domain('INVALID_ADDRESS','Expected finite 1-based rectangle');
}
function jobOptions(options:RecalcJobOptions):RecalcJobOptions {
  return {collectDeltas:options.collectDeltas,maxDeltaRecords:options.maxDeltaRecords,priority:options.priority};
}
function intersection(a:FiniteRangeAddress,b:FiniteRangeAddress):FiniteRangeAddress|null{
  if(a.sheet!==b.sheet)return null;
  const r={sheet:a.sheet,startRow:Math.max(a.startRow,b.startRow),startColumn:Math.max(a.startColumn,b.startColumn),endRow:Math.min(a.endRow,b.endRow),endColumn:Math.min(a.endColumn,b.endColumn)};
  return r.startRow<=r.endRow && r.startColumn<=r.endColumn?r:null;
}
function union(rects:FiniteRangeAddress[]):FiniteRangeAddress[] {
  if(!rects.length)return [];
  return [{sheet:rects[0].sheet,startRow:Math.min(...rects.map(r=>r.startRow)),startColumn:Math.min(...rects.map(r=>r.startColumn)),endRow:Math.max(...rects.map(r=>r.endRow)),endColumn:Math.max(...rects.map(r=>r.endColumn))}];
}
interface HostSubscription {
  id:string; options:Omit<ViewportSubscriptionOptions,'onFrame'|'onError'>;
  awaiting:number; seq:number; lastSent:number; lastPending?:boolean; lastCount?:number;
  rects:FiniteRangeAddress[]; full:boolean; dirty:boolean;
  reason:ViewportFrameBase['reason']; report?:StepReport; timer?:ReturnType<typeof setTimeout>;
}

/** Attach one workbook host. Requests remain FIFO, including asynchronous module imports. */
export async function attachWorkbookWorker(endpoint:WorkerEndpoint,options:AttachWorkbookWorkerOptions={}):Promise<{dispose():void}>{
  // Install the listener before awaiting initialization so a freshly spawned worker cannot lose its handshake.
  let ready:Promise<void>;
  let runtime:typeof import('./index.js');
  let wb:WorkbookApi;
  const sheets=new Map<string,ReturnType<WorkbookApi['sheet']>>();
  const sessions=new Map<string,SheetPortSessionApi>();
  const functions=new Map<string,{callback:Callback;options?:CustomFunctionOptions}>();
  const subs=new Map<string,HostSubscription>();
  let sessionId=0, disposed=false, auto=false, budget:number|null=5,maxDeltas=1024;
  let state=initialState(), job:RecalcJob|undefined,origin:'auto'|'explicit'|undefined;
  let evaluating=0, mayHaveFormulas=false;
  let driver=false, driverGeneration=0;
  let timer:ReturnType<typeof setTimeout>|undefined;
  let priorities:FiniteRangeAddress[]=[];
  const header=():WorkerStateHeader=>({...state,hasActiveJob:!!job?.active});
  const send=(m:any,transfer:any[]=[])=>endpoint.postMessage({protocol:'formualizer-worker',...m,state:header()},transfer);
  const status=()=>send({type:'status'});
  const sheet=(name:string)=>{
    if(!wb.sheetNames().includes(name))throw domain('INVALID_ADDRESS','Unknown sheet: '+name);
    let handle=sheets.get(name);if(!handle){handle=wb.sheet(name);sheets.set(name,handle);}return handle;
  };
  const allPriorities=()=>[...priorities,...[...subs.values()].filter(s=>s.options.prioritize!==false).map(s=>s.options.rect)];
  const updatePriority=()=>job?.prioritize(allPriorities());
  const stopTimer=()=>{if(timer!==undefined){clearTimeout(timer);timer=undefined;}};
  const releaseJob=(cancel:boolean)=>{
    stopTimer();
    const old=job;job=undefined;origin=undefined;
    if(!old)return null;
    try{return cancel && old.active?old.cancel():null;}finally{old.free();}
  };
  const cleanupWorkbook=()=>{
    releaseJob(true);driver=false;driverGeneration++;
    for(const handle of sessions.values())handle.free();sessions.clear();
    for(const handle of sheets.values())handle.free();sheets.clear();
    wb?.free();
  };
  const register=(name:string,value:Callback|{callback:Callback;options?:CustomFunctionOptions})=>{
    const def=typeof value==='function'?{callback:value}:value;
    if(!def||typeof def.callback!=='function')throw domain('INVALID_OPTIONS','Invalid custom function '+name);
    wb.registerFunction(name,def.callback,def.options);
    functions.set(name.toUpperCase(),def);
  };
  const registerAll=(map:WorkerFunctions)=>{for(const [name,value] of Object.entries(map))register(name,value);};
  function createJob(opts:RecalcJobOptions={},source:'auto'|'explicit'='explicit'){
    priorities=opts.priority??priorities;
    job=wb.beginRecalc({...opts,collectDeltas:opts.collectDeltas??true,maxDeltaRecords:opts.maxDeltaRecords??maxDeltas,priority:allPriorities()});
    origin=source;
  }
  function scheduleAuto(){
    if(!auto||disposed||driver||evaluating||state.recalcStatus==='paused'||(job&&origin!=='auto')||timer!==undefined)return;
    timer=setTimeout(()=>{
      timer=undefined;
      if(disposed||!auto||driver||evaluating||state.recalcStatus==='paused')return;
      try{if(!job)createJob({},'auto');state.recalcStatus='calculating';status();autoStep();}
      catch(error){state.recalcStatus='error';status();send({type:'host:error',error:serializeWorkerError(error)});}
    },0);
  }
  function autoStep(){
    timer=undefined;
    if(disposed||!job||origin!=='auto'||state.recalcStatus!=='calculating')return;
    try{step(budget);if(job)timer=setTimeout(autoStep,0);}
    catch(error){send({type:'host:error',error:serializeWorkerError(error)});}
  }
  function flush(sub:HostSubscription,force=false):ViewportFrame|undefined{
    if(!sub.dirty)return;
    const delay=(sub.options.throttleMs??16)-(Date.now()-sub.lastSent);
    if(!force&&(sub.awaiting||delay>0)){
      if(!sub.awaiting&&!sub.timer)sub.timer=setTimeout(()=>{sub.timer=undefined;safeFlush(sub);},Math.max(0,delay));
      return;
    }
    if(sub.timer){clearTimeout(sub.timer);sub.timer=undefined;}
    const r=sub.options.rect; const rows=r.endRow-r.startRow+1,cols=r.endColumn-r.startColumn+1;
    const changed=union(sub.rects);
    const full=sub.full || changed.some(c=>(c.endRow-c.startRow+1)*(c.endColumn-c.startColumn+1)>rows*cols/2);
    const pending=wb.pendingCellsInRect(r), hasPending=wb.hasPendingInRect(r);
    const base:ViewportFrameBase={...header(),subscriptionId:sub.id,frameSeq:++sub.seq,rect:r,hasInFlightMutations:false,
      reason:sub.reason,phase:sub.report?.phase??'Idle',progress:sub.report?.progress??null,
      changedRects:full?[r]:changed,fullRefresh:full,hasPending,
      ...(sub.options.includePendingCells===false?{}:{pendingCells:pending})};
    const mode=sub.options.mode??'values';
    let frame:ViewportFrame; const transfer:ArrayBuffer[]=[];
    if(mode==='deltas')frame={...base,mode};
    else if(mode==='snapshots')frame={...base,mode,snapshots:wb.rangePage(r,{offset:0,limit:rows*cols,includeValues:true}).items};
    else if(mode==='columnar'){
      const tags=new Uint8Array(rows*cols),numbers=new Float64Array(rows*cols),pendingBitset=new Uint8Array(Math.ceil(rows*cols/8)),strings:string[]=[];
      const table=new Map<string,number>();
      const text=(value:string)=>{let n=table.get(value);if(n===undefined){n=strings.length;table.set(value,n);strings.push(value);}return n;};
      const grid=sheet(r.sheet).readRange(r.startRow,r.startColumn,r.endRow,r.endColumn);
      for(let i=0;i<rows*cols;i++){
        const value=grid[Math.floor(i/cols)]?.[i%cols];
        if(value===null||value===undefined)continue;
        if(typeof value==='number'){tags[i]=1;numbers[i]=value;}
        else if(typeof value==='boolean'){tags[i]=2;numbers[i]=Number(value);}
        else if(typeof value==='string'){tags[i]=/^#(?:REF!|VALUE!|DIV\/0!|N\/A|NAME\?|NUM!|NULL!|SPILL!|CIRC!|CALC!)/.test(value)?4:3;numbers[i]=text(value);}
        else{tags[i]=value.kind==='error'?4:5;numbers[i]=text(JSON.stringify(value));}
      }
      for(const c of pending){const i=(c.row-r.startRow)*cols+c.column-r.startColumn;pendingBitset[i>>3]|=1<<(i&7);}
      frame={...base,mode,columnar:{rect:r,rows,cols,tags,numbers,strings,pendingBitset}};
      transfer.push(tags.buffer,numbers.buffer,pendingBitset.buffer);
    }else if(full)frame={...base,mode,grid:sheet(r.sheet).readRange(r.startRow,r.startColumn,r.endRow,r.endColumn)};
    else{
      const patches:ViewportCellPatch[]=[];
      for(const c of changed){
        const grid=sheet(c.sheet).readRange(c.startRow,c.startColumn,c.endRow,c.endColumn);
        for(let row=c.startRow;row<=c.endRow;row++)for(let column=c.startColumn;column<=c.endColumn;column++)
          patches.push({row,column,value:grid[row-c.startRow]?.[column-c.startColumn]});
      }
      frame={...base,mode,patches};
    }
    sub.lastPending=hasPending;sub.lastCount=sub.options.includePendingCells===false?undefined:pending.length;sub.dirty=false;sub.full=false;sub.rects=[];
    sub.lastSent=Date.now();sub.awaiting=frame.frameSeq;
    // refresh() also returns a frame: clone before transferring its columnar buffers.
    const returned=structuredClone(frame);
    send({type:'viewport',id:sub.id,frame},transfer);
    return returned;
  }
  function safeFlush(sub:HostSubscription,force=false){
    try{return flush(sub,force);}catch(error){sub.dirty=false;send({type:'viewport',id:sub.id,error:serializeWorkerError(error)});}
  }
  function notify(reason:ViewportFrameBase['reason'],report?:StepReport,rects?:FiniteRangeAddress[],full=false){
    for(const sub of subs.values()){
      const r=sub.options.rect;
      if(!wb.sheetNames().includes(r.sheet))continue;
      const overflow=report?.overflowSheets.includes(r.sheet);
      const changed=(rects??report?.changed??[]).map(c=>intersection(r,c)).filter((c):c is FiniteRangeAddress=>!!c);
      const pending=wb.hasPendingInRect(r);
      const count=sub.options.includePendingCells===false?undefined:wb.pendingCellsInRect(r).length;
      const terminal=report?.phase==='Finished'||report?.phase==='Cancelled';
      if(!full&&!overflow&&!changed.length&&pending===sub.lastPending&&count===sub.lastCount&&!terminal)continue;
      sub.rects=union([...sub.rects,...changed]);sub.full ||=full||!!overflow;sub.dirty=true;
      sub.reason=terminal?'finished':reason;sub.report=report;
      safeFlush(sub,terminal);
    }
  }
  function step(ms:number|null|undefined){
    if(!job)throw domain('NO_ACTIVE_JOB');
    validateBudget(ms);
    try{
      const report=job.step(ms);
      if(report.phase==='Finished'||report.phase==='Cancelled'){
        state.recalcStatus=report.phase==='Finished'?'idle':'cancelled';
        if(report.phase==='Finished')state.evaluatedRevision=state.revision;
        releaseJob(false);
      }
      notify('step',report);status();
      return {...report,...header()};
    }catch(error){releaseJob(true);state.recalcStatus='error';status();throw error;}
  }
  function cancel(){
    const report=releaseJob(true);driver=false;driverGeneration++;state.recalcStatus='cancelled';
    if(report)notify('finished',report);status();
    return report?{...report,...header()}:null;
  }
  function mutate(clientSeq:number|undefined,reset=false,changed?:FiniteRangeAddress[]){
    state.revision=reset?0:state.revision+1;
    if(reset)state.evaluatedRevision=0;
    state.ackClientSeq=Math.max(state.ackClientSeq,clientSeq??0);
    if(!mayHaveFormulas)state.evaluatedRevision=state.revision;
    notify('mutation',undefined,changed,changed===undefined);
    scheduleAuto();return header();
  }
  function makeSession(manifest:FioManifest|string):SheetPortSessionApi{
    return typeof manifest==='string'?runtime.SheetPortSession.fromManifestYaml(manifest,wb):runtime.SheetPortSession.fromManifest(manifest,wb);
  }
  function oneShot<T>(fn:()=>T,all=false):T{
    const implicit=origin==='auto';
    if(implicit)releaseJob(true);
    evaluating++;
    try{const result=fn();if(all)state.evaluatedRevision=state.revision;return result;}
    finally{evaluating--;if(implicit)state.recalcStatus='idle';if(!job)scheduleAuto();}
  }
  async function dispatch(method:string,a:any[],clientSeq?:number):Promise<any>{
    if(disposed && method!=='dispose')throw new Error('Workbook worker disposed');
    if(method==='initialize'){
      validateBudget(a[1]);auto=a[0];budget=a[1]??5;maxDeltas=a[2]??1024;return header();
    }
    if(method.startsWith('load')){
      let next:WorkbookApi;
      if(method==='loadEmpty')next=new runtime.Workbook(a[0]);
      else if(method==='loadFromJson')next=runtime.Workbook.fromJson(a[0],a[1]);
      else if(method==='loadFromXlsxBytes')next=runtime.Workbook.fromXlsxBytes(a[0],a[1]);
      else throw domain('INVALID_OPTIONS','Unknown loader');
      cleanupWorkbook();wb=next;state.recalcStatus='idle';priorities=[];mayHaveFormulas=method!=='loadEmpty';
      if(options.clearFunctionsOnLoad){functions.clear();registerAll(options.functions??{});}
      else for(const [name,def] of functions)wb.registerFunction(name,def.callback,def.options);
      return mutate(clientSeq,true);
    }
    if(method==='dispose'){cleanupWorkbook();disposed=true;
      for(const sub of subs.values())if(sub.timer)clearTimeout(sub.timer);subs.clear();return;}
    if(method==='setAutoRecalc'){
      validateBudget(a[1]?.budgetMs);auto=a[0];budget=a[1]?.budgetMs??budget;maxDeltas=a[1]?.maxDeltaRecords??maxDeltas;
      if(auto)scheduleAuto();else if(origin==='auto'){releaseJob(true);state.recalcStatus='idle';}return header();
    }
    if(method==='beginRecalc'||method==='drive:start'){
      const opts=a[0]??{};
      if(driver || (job&&origin==='explicit'&&method==='beginRecalc'))throw domain('ALREADY_PENDING','An explicit job is already active',{explicit:true});
      if(!job)createJob(opts);else{priorities=opts.priority??priorities;updatePriority();}
      stopTimer();origin='explicit';driver=method==='drive:start';if(driver)driverGeneration++;
      state.recalcStatus=driver?'calculating':'paused';status();return {...header(),driverId:driverGeneration};
    }
    if(method==='drive:next'){
      if(a[0]!==driverGeneration||!driver||state.recalcStatus!=='calculating'||!job)return null;
      return step(a[1]===undefined?5:a[1]);
    }
    if(method==='drive:abort'){if(driver&&a[0]===driverGeneration)return cancel();return null;}
    if(method==='drive:end'){
      if(a[0]!==driverGeneration)return header();
      driver=false;if(job){if(a[1])state.recalcStatus='paused';else cancel();}status();return header();
    }
    if(method==='stepRecalc'){if(driver)throw domain('ALREADY_PENDING','A driver owns the job',{explicit:true});return step(a[0]);}
    if(method==='pauseRecalc'){stopTimer();if(job){origin='explicit';state.recalcStatus='paused';}status();return header();}
    if(method==='cancelRecalc')return cancel();
    if(method==='prioritize'){for(const r of a[0])validateRect(r);priorities=a[0];updatePriority();return header();}
    if(method==='viewport:subscribe'){
      const [id,opts]=a;validateRect(opts.rect);sheet(opts.rect.sheet);
      if(!['values','snapshots','deltas','columnar'].includes(opts.mode??'values')||!Number.isFinite(opts.throttleMs??16)||(opts.throttleMs??16)<0)throw domain('INVALID_OPTIONS','Invalid viewport options');
      const sub:HostSubscription={id,options:opts,awaiting:0,seq:0,lastSent:0,rects:[],full:true,dirty:opts.immediate!==false,reason:'initial'};
      subs.set(id,sub);updatePriority();if(sub.dirty)safeFlush(sub,true);return;
    }
    if(method.startsWith('viewport:')){
      const sub=subs.get(a[0]);if(!sub)throw domain('INVALID_OPTIONS','Unknown viewport');
      if(method==='viewport:unsubscribe'){if(sub.timer)clearTimeout(sub.timer);subs.delete(sub.id);updatePriority();return;}
      if(method==='viewport:rect'){validateRect(a[1]);sheet(a[1].sheet);sub.options.rect=a[1];sub.reason='rect-change';updatePriority();}
      else sub.reason='manual';
      sub.dirty=true;sub.full=true;sub.rects=[];return safeFlush(sub,true);
    }
    if(method==='session:create'){const id='s'+(++sessionId);sessions.set(id,makeSession(a[0]));return id;}
    if(method.startsWith('session:')){
      const session=sessions.get(a[0]);
      if(method==='session:dispose'){session?.free();sessions.delete(a[0]);return;}
      if(!session)throw domain('INVALID_OPTIONS','SheetPort session is disposed or belongs to a previous workbook');
      const operation=method.slice(8);
      if(operation==='writeInputs'){session.writeInputs(a[1]);return mutate(clientSeq);}
      if(operation==='evaluateOnce')return oneShot(()=>session.evaluateOnce(a[1]));
      if(!['manifest','describePorts','readInputs','readOutputs'].includes(operation))throw domain('INVALID_OPTIONS');
      return (session as any)[operation]();
    }
    if(method==='evaluateSheetPort'){
      if(!a[1]?.inputs)state.ackClientSeq=Math.max(state.ackClientSeq,clientSeq??0);
      return oneShot(()=>{const session=makeSession(a[0]);
        try{if(a[1]?.inputs){session.writeInputs(a[1].inputs);mutate(clientSeq);}return session.evaluateOnce(a[1]?.evaluateOptions);}
        finally{session.free();}
      });
    }
    if(method==='recalculateXlsxBytes')return runtime.recalculateXlsxBytes(a[0],a[1],a[2]);
    if(method==='tokenize'){const tokenizer=await runtime.tokenize(a[0],a[1]);return tokenizer.tokens;}
    if(method==='parse')return runtime.parse(a[0],a[1]);
    if(method==='registerFunctionModule'){
      const module=await (options.importModule??(url=>import(url)))(a[0]);
      const map=module[a[1]?.exportName??'functions']??module.default??module;
      registerAll(map as WorkerFunctions);mutate(clientSeq);return wb.listFunctions();
    }
    if(method==='registerFunctionFromSource'){
      if(!options.allowFunctionFromSource)throw domain('INVALID_OPTIONS','Function source registration is disabled');
      const callback=new Function('return ('+a[1]+'\n)')();
      register(a[0],{callback,options:a[2]});mutate(clientSeq);
      return wb.listFunctions().find(f=>f.name===a[0].toUpperCase());
    }
    if(method==='editBatch'){
      if(!Array.isArray(a[0]))throw domain('INVALID_OPTIONS','Expected edit operations');
      // Validate before opening an undo action; engine errors still close the action in finally.
      for(const op of a[0])if(!['setValue','setFormula','setValues','setFormulas','addSheet'].includes(op.op??op.kind))throw domain('INVALID_OPTIONS','Unknown edit operation');
      if(a[1]?.actionDescription!==undefined)wb.beginAction(a[1].actionDescription);
      mayHaveFormulas ||=a[0].some((op:any)=>['setFormula','setFormulas'].includes(op.op??op.kind));
      try{for(const op of a[0]){
        const kind=op.op??op.kind;
        if(kind==='addSheet')wb.addSheet(op.name);
        else if(kind==='setValues'||kind==='setFormulas') (sheet(op.sheet) as any)[kind](op.startRow,op.startCol,op.values??op.formulas);
        else (sheet(op.sheet) as any)[kind](op.row,op.column??op.col,op.value??(kind==='setValue'?null:op.formula));
      }}finally{if(a[1]?.actionDescription!==undefined)wb.endAction();}
      return mutate(clientSeq);
    }
    if(['setValue','setFormula','setValues','setFormulas'].includes(method)){
      if(method==='setFormula'||method==='setFormulas')mayHaveFormulas=true;
      (sheet(a[0]) as any)[method](...a.slice(1));
      const block=method==='setValues'||method==='setFormulas';
      const height=block?a[3].length:1,width=block?Math.max(0,...a[3].map((r:any[])=>r.length)):1;
      return mutate(clientSeq,false,height&&width?[{sheet:a[0],startRow:a[1],startColumn:a[2],endRow:a[1]+height-1,endColumn:a[2]+width-1}]:[]);
    }
    if(['getValue','getValueWithState','getFormula','readRange','readRangeWithState'].includes(method)){
      if(method==='readRangeWithState'){const r=a[0];validateRect(r);return {data:sheet(r.sheet).readRange(r.startRow,r.startColumn,r.endRow,r.endColumn),...header()};}
      const data=(sheet(a[0]) as any)[method==='getValueWithState'?'getValue':method](...a.slice(1));
      return method==='getValueWithState'?{data,...header()}:data;
    }
    if(!(workbookMethods as readonly string[]).includes(method))throw domain('INVALID_OPTIONS','Unknown worker method '+method);
    if(method==='unregisterFunction'){wb.unregisterFunction(a[0]);functions.delete(a[0].toUpperCase());return mutate(clientSeq);}
    if(['evaluateAll','evaluateCell','evaluateCells'].includes(method)){
      if(method==='evaluateCell')sheet(a[0]);
      if(method==='evaluateCells')a[0]=a[0].map((c:any)=>{const t=Array.isArray(c)?c:[c.sheet,c.row,c.column];sheet(t[0]);return t;});
      const result=oneShot(()=>(wb as any)[method](...a),method==='evaluateAll');
      notify('mutation',undefined,undefined,true);return result;
    }
    if(method==='getEvalPlan')a[0]=a[0].map((c:any)=>Array.isArray(c)?c:[c.sheet,c.row,c.column]);
    const result=(wb as any)[method](...a);
    return mutationMethods.has(method)?mutate(clientSeq):result;
  }
  let queue=Promise.resolve();
  const unlisten=listen(endpoint,m=>{
    if(m?.protocol!=='formualizer-worker')return;
    if(m.type==='hello'){void ready.then(()=>send({type:'ready'})).catch(()=>{});return;}
    if(m.type==='viewport:ack'){
      const sub=subs.get(m.id);
      if(sub&&sub.awaiting===m.frameSeq){sub.awaiting=0;safeFlush(sub);}return;
    }
    if(m.type!=='request')return;
    queue=queue.then(async()=>{
      try{
        await ready;
        const data=await dispatch(m.method,m.args??[],m.clientSeq);
        const transfers:ArrayBuffer[]=[];
        if(m.method==='recalculateXlsxBytes'&&m.args[3]!==false&&data?.bytes){
          const input=prepareTransferableBytes(data.bytes,true);data.bytes=input.bytes;transfers.push(...input.transfer);
        }
        send({type:'reply',id:m.id,data},transfers);
        if(m.method==='dispose'){unlisten();endpoint.close?.();}
      }catch(error){
        if(m.clientSeq)state.ackClientSeq=Math.max(state.ackClientSeq,m.clientSeq);
        send({type:'reply',id:m.id,error:serializeWorkerError(error)});
      }
    });
  },()=>{dispose();});
  const dispose=()=>{
    if(disposed)return;disposed=true;unlisten();cleanupWorkbook();
    for(const sub of subs.values())if(sub.timer)clearTimeout(sub.timer);subs.clear();endpoint.close?.();
  };
  ready=(async()=>{
    runtime=options.runtime??await import('./index.js');await runtime.default();
    if(disposed)return;
    wb=new runtime.Workbook();registerAll(options.functions??{});
    send({type:'ready'});
  })();
  try{await ready;}catch(error){unlisten();throw error;}
  return {dispose};
}
