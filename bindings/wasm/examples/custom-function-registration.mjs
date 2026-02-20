import init, { Workbook } from "formualizer";

await init();

const wb = new Workbook();
wb.addSheet("Sheet1");

wb.setValue("Sheet1", 1, 1, 1);
wb.setValue("Sheet1", 1, 2, 2);
wb.setValue("Sheet1", 2, 1, 3);
wb.setValue("Sheet1", 2, 2, 4);

wb.registerFunction(
  "js_sum_range",
  (grid) => grid.flat().reduce((total, value) => total + value, 0),
  { minArgs: 1, maxArgs: 1 },
);

wb.setFormula("Sheet1", 1, 3, "=JS_SUM_RANGE(A1:B2)");
wb.evaluateAll();

console.log(wb.sheet("Sheet1").getValue(1, 3));
console.log(wb.listFunctions());

wb.unregisterFunction("js_sum_range");
