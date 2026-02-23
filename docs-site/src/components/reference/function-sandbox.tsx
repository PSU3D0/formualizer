'use client';

import { useState, useEffect, useCallback } from 'react';
import { DynamicCodeBlock } from 'fumadocs-ui/components/dynamic-codeblock';

let wasmPromise: Promise<any> | null = null;
function loadWasm() {
  if (!wasmPromise) {
    wasmPromise = import('formualizer').then(async (m) => {
      await m.default();
      return m;
    });
  }
  return wasmPromise;
}

export interface FunctionSandboxProps {
  title?: string;
  formula: string;
  grid?: Record<string, string | number | boolean | null>;
  expected?: string;
}

export function FunctionSandbox({ title, formula, grid = {}, expected }: FunctionSandboxProps) {
  const [result, setResult] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isRunning, setIsRunning] = useState(false);
  const [hasRun, setHasRun] = useState(false);
  const [evalMs, setEvalMs] = useState<number | null>(null);

  const [localFormula, setLocalFormula] = useState(formula);
  const [localGrid, setLocalGrid] = useState(grid);
  const [activeSheet, setActiveSheet] = useState('Sheet1');
  const [sheets, setSheets] = useState(['Sheet1']);

  useEffect(() => {
    setLocalFormula(formula);
    setLocalGrid(grid);
    setHasRun(false);
    setResult(null);
    setEvalMs(null);
    // Scan grid for other sheets if any (advanced use case)
    const detectedSheets = new Set(['Sheet1']);
    Object.keys(grid).forEach(key => {
        if (key.includes('!')) {
            detectedSheets.add(key.split('!')[0]);
        }
    });
    setSheets(Array.from(detectedSheets));
  }, [formula, grid]);

  const runFormula = useCallback(async () => {
    setIsRunning(true);
    setError(null);
    try {
      const formualizer = await loadWasm();
      const start = performance.now();
      const wb = new formualizer.Workbook();
      
      // Ensure all sheets exist
      sheets.forEach(s => {
          try { wb.addSheet(s); } catch(e) {}
      });
      
      // Populate inputs
      for (const [cellRef, val] of Object.entries(localGrid)) {
        let sheetName = 'Sheet1';
        let ref = cellRef;
        if (cellRef.includes('!')) {
            const parts = cellRef.split('!');
            sheetName = parts[0];
            ref = parts[1];
        }

        const match = ref.match(/^([A-Z]+)([0-9]+)$/i);
        if (match) {
          const colLetter = match[1].toUpperCase();
          const row = parseInt(match[2], 10);
          let col = 0;
          for (let i = 0; i < colLetter.length; i++) {
            col = col * 26 + (colLetter.charCodeAt(i) - 64);
          }
          
          if (val != null) {
            const sVal = String(val);
            const num = Number(sVal);
            const finalVal = (!isNaN(num) && sVal.trim() !== '') ? num : val;
            wb.setValue(sheetName, row, col, finalVal);
          }
        }
      }

      wb.setFormula('Sheet1', 100, 100, localFormula);
      wb.evaluateAll();
      const computed = wb.evaluateCell('Sheet1', 100, 100);
      
      setEvalMs(performance.now() - start);
      
      if (computed !== undefined && computed !== null) {
        setResult(String(computed));
      } else {
        setResult('null');
      }
      setHasRun(true);
    } catch (e: any) {
      setError(e.toString());
    } finally {
      setIsRunning(false);
    }
  }, [localFormula, localGrid, sheets]);

  const updateGridValue = (ref: string, val: string) => {
    setLocalGrid(prev => ({ ...prev, [ref]: val }));
    setHasRun(false);
  };

  const addSheet = () => {
      if (sheets.length >= 3) return;
      const nextName = `Sheet${sheets.length + 1}`;
      setSheets([...sheets, nextName]);
  };

  const removeCell = (ref: string) => {
    setLocalGrid(prev => {
      const next = { ...prev };
      delete next[ref];
      return next;
    });
    setHasRun(false);
  };

  const addCell = () => {
    // Limit to 8 cells per sheet to keep it clean
    if (activeGridItems.length >= 8) return;
    
    // Find a reasonable next cell name (A1, A2, etc)
    let nextNum = 1;
    while (Object.keys(localGrid).some(k => k.endsWith(`A${nextNum}`))) {
      nextNum++;
    }
    
    const ref = activeSheet === 'Sheet1' ? `A${nextNum}` : `${activeSheet}!A${nextNum}`;
    setLocalGrid(prev => ({ ...prev, [ref]: "" }));
    setHasRun(false);
  };

  // Filter grid items for the active sheet
  const activeGridItems = Object.entries(localGrid).filter(([ref]) => {
      if (ref.includes('!')) {
          return ref.startsWith(`${activeSheet}!`);
      }
      return activeSheet === 'Sheet1';
  });

  const getDisplayRef = (ref: string) => ref.includes('!') ? ref.split('!')[1] : ref;

  return (
    <div className="rounded-xl border bg-fd-card overflow-hidden my-6">
      {title ? (
        <div className="bg-fd-background/50 px-4 py-2 border-b text-sm font-medium text-fd-foreground flex justify-between items-center">
          <span>{title}</span>
          {evalMs !== null && (
              <span className="text-xs bg-amber-500/10 text-amber-600 dark:text-amber-400 border border-amber-500/20 px-2 py-0.5 rounded-full font-mono flex items-center gap-1.5 leading-none">
                  <span className="text-[10px]">⚡</span>
                  {evalMs.toFixed(2)}ms
              </span>
          )}
        </div>
      ) : null}

      <div className="grid md:grid-cols-2 divide-y md:divide-y-0 md:divide-x">
        {/* Left side: Inputs */}
        <div className="flex flex-col">
          <div className="p-4 space-y-4 flex-1">
            <div className="space-y-0">
              <div className="flex justify-between items-center bg-fd-muted/30 border border-b-0 rounded-t-md px-2.5 py-1.5">
                <div className="text-xs font-bold uppercase tracking-wider text-fd-muted-foreground">
                    Grid
                </div>
                <div className="flex gap-1 p-0.5 bg-fd-background/50 rounded-md border border-fd-border/50">
                    {sheets.map(s => (
                        <button
                            key={s}
                            onClick={() => setActiveSheet(s)}
                            className={`text-[10px] font-semibold px-2.5 py-1 rounded-sm transition-all ${activeSheet === s ? 'bg-fd-primary text-fd-primary-foreground shadow-sm' : 'text-fd-muted-foreground hover:bg-fd-muted'}`}
                        >
                            {s}
                        </button>
                    ))}
                    {sheets.length < 3 && (
                        <button 
                            onClick={addSheet}
                            title="Add Sheet"
                            className="text-[10px] px-2 py-1 rounded-sm text-fd-muted-foreground hover:bg-fd-muted hover:text-fd-primary transition-colors"
                        >
                            +
                        </button>
                    )}
                </div>
              </div>
              
              <div className="border bg-fd-background overflow-hidden">
                <table className="w-full text-sm border-collapse mt-0 mb-0">
                  <thead className="bg-fd-muted/50 border-b">
                    <tr>
                      <th className="px-3 py-1 text-left font-medium text-fd-muted-foreground w-16 border-r">Cell</th>
                      <th className="px-3 py-1 text-left font-medium text-fd-muted-foreground">Value</th>
                      <th className="w-8 border-l"></th>
                    </tr>
                  </thead>
                  <tbody className="divide-y">
                    {activeGridItems.length > 0 ? (
                        activeGridItems.map(([ref, val]) => (
                        <tr key={ref}>
                            <td className="p-0 border-r bg-fd-muted/20">
                              <input 
                                  className="w-full px-3 py-1 bg-transparent border-none outline-none font-mono text-xs text-fd-muted-foreground focus:ring-1 focus:ring-fd-primary/30"
                                  value={getDisplayRef(ref)}
                                  onChange={(e) => {
                                    const newPart = e.target.value.toUpperCase();
                                    const newRef = ref.includes('!') ? `${ref.split('!')[0]}!${newPart}` : newPart;
                                    setLocalGrid(prev => {
                                      const next = { ...prev };
                                      const val = next[ref];
                                      delete next[ref];
                                      next[newRef] = val;
                                      return next;
                                    });
                                    setHasRun(false);
                                  }}
                              />
                            </td>
                            <td className="p-0">
                            <input 
                                className="w-full px-3 py-1 bg-transparent border-none outline-none font-mono text-xs focus:ring-1 focus:ring-fd-primary/30"
                                value={String(val)}
                                onChange={(e) => updateGridValue(ref, e.target.value)}
                            />
                            </td>
                            <td className="p-0 border-l">
                              <button 
                                onClick={() => removeCell(ref)}
                                className="w-full h-full flex items-center justify-center text-fd-muted-foreground hover:text-red-500 transition-colors py-1"
                                title="Remove Cell"
                              >
                                <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M18 6L6 18M6 6l12 12"/></svg>
                              </button>
                            </td>
                        </tr>
                        ))
                    ) : (
                        <tr>
                            <td colSpan={3} className="px-3 py-4 text-center text-xs text-fd-muted-foreground italic bg-fd-muted/5">
                                No inputs on {activeSheet}.
                            </td>
                        </tr>
                    )}
                  </tbody>
                </table>
              </div>
              <div className="border border-t-0 rounded-b-md bg-fd-muted/10 px-2 py-1.5 flex justify-center">
                  <button 
                      className="text-xs font-medium text-fd-muted-foreground hover:text-fd-primary flex items-center gap-1.5 transition-colors"
                      onClick={addCell}
                  >
                      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M12 5v14M5 12h14"/></svg>
                      Add cell
                  </button>
              </div>
            </div>

            <div className="space-y-2 pt-2">
              <div className="text-xs font-medium uppercase tracking-wide text-fd-muted-foreground">
                Formula
              </div>
              <div className="flex rounded-md border bg-fd-background focus-within:ring-1 focus-within:ring-fd-primary/30 overflow-hidden">
                 <span className="px-2 py-1.5 bg-fd-muted/50 border-r text-fd-muted-foreground font-mono text-sm">=</span>
                 <input 
                   className="flex-1 px-2 py-1.5 bg-transparent border-none outline-none font-mono text-sm"
                   value={localFormula.startsWith('=') ? localFormula.substring(1) : localFormula} 
                   onChange={(e) => {
                     const val = e.target.value;
                     setLocalFormula(val.startsWith('=') ? val : '=' + val);
                     setHasRun(false);
                   }}
                 />
              </div>
            </div>
            
            <button 
              onClick={runFormula}
              disabled={isRunning || hasRun}
              className={`flex items-center gap-2 rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
                hasRun 
                  ? 'bg-fd-muted text-fd-muted-foreground cursor-default' 
                  : 'bg-fd-primary text-fd-primary-foreground hover:bg-fd-primary/90 shadow-sm'
              }`}
            >
              <svg width="12" height="12" viewBox="0 0 16 16" fill="currentColor">
                <path d="M4 2v12l10-6L4 2z" />
              </svg>
              {isRunning ? 'Running...' : hasRun ? 'Evaluation Complete' : 'Run in Browser'}
            </button>
          </div>
        </div>

        {/* Right side: Output */}
        <div className="p-4 bg-fd-background/20">
           <div className="text-xs font-medium uppercase tracking-wide text-fd-muted-foreground mb-2">
             Result
           </div>
           
           <div className="space-y-3">
             {result !== null && !error && (
               <div className="rounded-md border border-green-500/30 bg-green-500/10 px-4 py-3 text-green-800 dark:text-green-300 font-mono text-sm break-all shadow-sm">
                 {result}
               </div>
             )}

             {error && (
               <div className="rounded-md border border-red-500/30 bg-red-500/10 px-4 py-3 text-red-800 dark:text-red-300 font-mono text-xs break-words shadow-sm">
                 {error}
               </div>
             )}

             {!result && !error && (
               <div className="rounded-md border border-dashed border-fd-border px-4 py-3 text-fd-muted-foreground text-sm italic">
                 Not evaluated yet.
               </div>
             )}

             {expected !== undefined && (
               <div className="mt-4 pt-4 border-t border-fd-border">
                 <div className="text-xs font-medium uppercase tracking-wide text-fd-muted-foreground mb-1">
                   Expected
                 </div>
                 <div className="font-mono text-xs text-fd-muted-foreground">
                   {String(expected)}
                 </div>
               </div>
             )}
           </div>
        </div>
      </div>
    </div>
  );
}
