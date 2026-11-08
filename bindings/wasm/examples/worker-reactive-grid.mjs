import {createInProcessWorkbookWorker} from 'formualizer/worker';

export async function reactiveGridExample() {
  const client=await createInProcessWorkbookWorker({autoRecalc:true,autoRecalcBudgetMs:0});
  const rect={sheet:'Grid',startRow:1,startColumn:1,endRow:8,endColumn:3};
  const frames=[];
  try {
    await client.addSheet('Grid');
    const values=await client.subscribeViewport({rect,mode:'values',throttleMs:16,onFrame(frame) {
      // Keep optimistic entries until their clientSeq <= frame.ackClientSeq.
      // grid replaces the viewport; patches update row/column coordinates.
      frames.push({revision:frame.revision,ack:frame.ackClientSeq,pending:frame.hasPending,
        optimistic:frame.hasInFlightMutations,payload:frame.grid?'grid':'patches'});
    }});
    const canvas=await client.subscribeViewport({rect,mode:'columnar',onFrame(frame) {
      // Row-major tags/numbers and pendingBitset can be consumed directly by a canvas grid.
      if(frame.mode==='columnar')console.assert(frame.columnar.tags.length===24);
    }});
    await client.setFormula('Grid',2,1,'=A1*2');
    const edits=Array.from({length:12},(_,i)=>client.setValue('Grid',1,1,i));
    console.assert(client.hasInFlightMutations);
    await Promise.all(edits); await client.recalc({budgetMs:0});
    await values.setRect({...rect,startRow:2,endRow:9}); // Scroll and retarget job priority.
    await canvas.setRect({...rect,startRow:2,endRow:9});
    await values.refresh();await canvas.refresh();
    console.assert(await client.getValue('Grid',2,1)===22);
    await values.unsubscribe();await canvas.unsubscribe();
    return 'reactive grid: '+frames.length+' causal frames';
  } finally {await client.dispose();}
}
if(typeof process!=='undefined'&&process.argv[1]&&import.meta.url===new URL(process.argv[1],'file:').href)
 console.log(await reactiveGridExample());
