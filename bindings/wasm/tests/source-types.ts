// Compile against a private stage with sibling dist/pkg, never source pkg output.
import type {
  SourceRecalculateLimits, SourcePhysicalCounter, SourceUnavailableOperation,
  SourceModeVerification, SourceDependencyEvidence, SourceResourceError,
  XlsxRecalculateOptions,
} from '../dist/index.js';

const limits = {
  maxInputBytes: 1, maxEntries: 1, maxExpandedBytes: 1, maxWorksheetBytes: 1,
  maxFormulaCells: 1, maxOutputBytes: 1, maxXmlDepth: 1, maxCells: 1,
  maxColumns: 1, maxOperationRoots: 1, maxOperationTextBytes: 1,
  maxAffectedWork: 1, maxAffectedRegions: 1, maxAffectedScratchBytes: 1,
} satisfies SourceRecalculateLimits;
const options: XlsxRecalculateOptions = {limits, physicalTelemetry: true};
const noOverride: XlsxRecalculateOptions = {limits: null, calculation: null};
const counters: SourcePhysicalCounter[] = [null, 0, '18446744073709551615'];
const verification: SourceModeVerification[] = ['unavailable', 'unverified'];
// @ts-expect-error Explicit null fields do not mean no override.
const nullField: SourceRecalculateLimits = {maxCells: null};
// @ts-expect-error Limits have no decimal string domain.
const stringField: SourceRecalculateLimits = {maxAffectedWork: '17'};
// @ts-expect-error Telemetry is a strict boolean.
const stringTelemetry: XlsxRecalculateOptions = {physicalTelemetry: 'true'};

function describe(operation: SourceUnavailableOperation): string {
  switch (operation.kind) {
    case 'unknown_function': case 'unimplemented_function': case 'source_feature':
    case 'unverified_provider': return operation.name;
    case 'unimplemented_function_mode': case 'unverified_function_mode':
      return operation.name + ':' + operation.mode;
    case 'postfix_invocation': return operation.kind;
    case 'internal_result': return operation.token;
    default: { const exhaustive: never = operation; return exhaustive; }
  }
}
function evidenceWork(evidence: SourceDependencyEvidence): number {
  return evidence.queryWork + evidence.publicationMembershipWorkBound;
}
function resource(error: SourceResourceError): bigint {
  return BigInt(error.observed) - BigInt(error.limit);
}
void [options, noOverride, counters, verification, nullField, stringField,
  stringTelemetry, describe, evidenceWork, resource];
