import type { ASTNodeData, ReferenceData } from 'formualizer';

function colToLabel(col: number): string {
  if (col <= 0) return '?';
  let n = col;
  let out = '';
  while (n > 0) {
    const rem = (n - 1) % 26;
    out = String.fromCharCode(65 + rem) + out;
    n = Math.floor((n - 1) / 26);
  }
  return out;
}

function cellRef(row: number, col: number, rowAbs: boolean, colAbs: boolean): string {
  return `${colAbs ? '$' : ''}${colToLabel(col)}${rowAbs ? '$' : ''}${row}`;
}

export function formatReference(reference: ReferenceData): string {
  const start = cellRef(
    reference.rowStart,
    reference.colStart,
    reference.rowAbsStart,
    reference.colAbsStart,
  );
  const end = cellRef(
    reference.rowEnd,
    reference.colEnd,
    reference.rowAbsEnd,
    reference.colAbsEnd,
  );

  const core = start === end ? start : `${start}:${end}`;
  return reference.sheet ? `${reference.sheet}!${core}` : core;
}

function visit(node: ASTNodeData, onNode: (node: ASTNodeData) => void) {
  onNode(node);

  if (node.left) visit(node.left, onNode);
  if (node.right) visit(node.right, onNode);
  if (node.operand) visit(node.operand, onNode);

  if (node.args) {
    for (const arg of node.args) visit(arg, onNode);
  }

  if (node.elements) {
    for (const row of node.elements) {
      for (const item of row) visit(item, onNode);
    }
  }
}

export function extractReferences(ast: ASTNodeData): string[] {
  const refs = new Set<string>();
  visit(ast, (node) => {
    if (node.reference) refs.add(formatReference(node.reference));
  });
  return Array.from(refs);
}

export function extractFunctionNames(ast: ASTNodeData): string[] {
  const names = new Set<string>();
  visit(ast, (node) => {
    if (node.type === 'function' && node.name) {
      names.add(node.name.toUpperCase());
    }
  });
  return Array.from(names);
}

function label(node: ASTNodeData): string {
  switch (node.type) {
    case 'function':
      return `Function ${node.name ?? '(unknown)'}(${node.args?.length ?? 0})`;
    case 'reference':
      return `Reference ${node.reference ? formatReference(node.reference) : '(missing)'}`;
    case 'number':
    case 'text':
    case 'boolean':
      return `${node.type} ${String(node.value)}`;
    case 'binaryOp':
      return `BinaryOp ${node.op ?? '?'}`;
    case 'unaryOp':
      return `UnaryOp ${node.op ?? '?'}`;
    case 'array':
      return `Array ${node.elements?.length ?? 0} row(s)`;
    case 'error':
      return `Error ${node.message ?? '(unknown)'}`;
    default:
      return node.type;
  }
}

export function astTreeLines(ast: ASTNodeData): string[] {
  const lines: string[] = [];

  const walk = (node: ASTNodeData, prefix: string) => {
    lines.push(`${prefix}${label(node)}`);

    const children: ASTNodeData[] = [];
    if (node.left) children.push(node.left);
    if (node.right) children.push(node.right);
    if (node.operand) children.push(node.operand);
    if (node.args) children.push(...node.args);
    if (node.elements) {
      for (const row of node.elements) {
        children.push(...row);
      }
    }

    for (const child of children) {
      walk(child, `${prefix}  `);
    }
  };

  walk(ast, '');
  return lines;
}
