#ifndef FORMUALIZER_CFFI_H
#define FORMUALIZER_CFFI_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct fz_buffer {
    uint8_t *data;
    size_t len;
    size_t cap;
} fz_buffer;

typedef enum fz_status_code {
    FZ_STATUS_OK = 0,
    FZ_STATUS_ERROR = 1,
} fz_status_code;

typedef struct fz_status {
    fz_status_code code;
    fz_buffer error;
} fz_status;

typedef enum fz_encoding_format {
    FZ_ENCODING_JSON = 0,
    FZ_ENCODING_CBOR = 1,
} fz_encoding_format;

typedef enum fz_formula_dialect {
    FZ_DIALECT_EXCEL = 0,
    FZ_DIALECT_OPENFORMULA = 1,
} fz_formula_dialect;

typedef struct fz_parse_options {
    bool include_spans;
    fz_formula_dialect dialect;
} fz_parse_options;

typedef struct fz_workbook_h {
    void *ptr;
} fz_workbook_h;

void fz_buffer_free(fz_buffer buffer);

int fz_common_abi_version(void);
int fz_parse_abi_version(void);
int fz_workbook_abi_version(void);

fz_buffer fz_common_parse_range_a1(
    const char *input,
    fz_encoding_format format,
    fz_status *status);

fz_buffer fz_common_format_range_a1(
    const uint8_t *payload,
    size_t len,
    fz_encoding_format format,
    fz_status *status);

fz_buffer fz_common_normalize_literal_value(
    const uint8_t *payload,
    size_t len,
    fz_encoding_format format,
    fz_status *status);

fz_buffer fz_parse_tokenize(
    const char *formula,
    fz_parse_options options,
    fz_encoding_format format,
    fz_status *status);

fz_buffer fz_parse_ast(
    const char *formula,
    fz_parse_options options,
    fz_encoding_format format,
    fz_status *status);

fz_buffer fz_parse_canonical_formula(
    const char *formula,
    fz_formula_dialect dialect,
    fz_status *status);

fz_workbook_h fz_workbook_create(fz_status *status);
fz_workbook_h fz_workbook_open_xlsx(const char *path, fz_status *status);
void fz_workbook_free(fz_workbook_h wb);
void fz_workbook_add_sheet(fz_workbook_h wb, const char *name, fz_status *status);
void fz_workbook_delete_sheet(fz_workbook_h wb, const char *name, fz_status *status);
void fz_workbook_rename_sheet(
    fz_workbook_h wb,
    const char *old_name,
    const char *new_name,
    fz_status *status);
int fz_workbook_has_sheet(fz_workbook_h wb, const char *name, fz_status *status);

fz_buffer fz_workbook_sheet_names(
    fz_workbook_h wb,
    fz_encoding_format format,
    fz_status *status);

fz_buffer fz_workbook_sheet_dimensions(
    fz_workbook_h wb,
    const char *name,
    fz_encoding_format format,
    fz_status *status);

void fz_workbook_set_cell_value(
    fz_workbook_h wb,
    const char *sheet,
    uint32_t row,
    uint32_t col,
    const uint8_t *value_payload,
    size_t len,
    fz_encoding_format format,
    fz_status *status);

void fz_workbook_set_cell_formula(
    fz_workbook_h wb,
    const char *sheet,
    uint32_t row,
    uint32_t col,
    const char *formula,
    fz_status *status);

fz_buffer fz_workbook_get_cell_value(
    fz_workbook_h wb,
    const char *sheet,
    uint32_t row,
    uint32_t col,
    fz_encoding_format format,
    fz_status *status);

fz_buffer fz_workbook_get_cell_formula(
    fz_workbook_h wb,
    const char *sheet,
    uint32_t row,
    uint32_t col,
    fz_status *status);

fz_buffer fz_workbook_evaluate_all(
    fz_workbook_h wb,
    fz_encoding_format format,
    fz_status *status);

fz_buffer fz_workbook_evaluate_cells(
    fz_workbook_h wb,
    const uint8_t *targets_payload,
    size_t len,
    fz_encoding_format format,
    fz_status *status);

fz_buffer fz_workbook_read_range(
    fz_workbook_h wb,
    const uint8_t *range_payload,
    size_t len,
    fz_encoding_format format,
    fz_status *status);

void fz_workbook_set_values(
    fz_workbook_h wb,
    const char *sheet,
    uint32_t start_row,
    uint32_t start_col,
    const uint8_t *values_payload,
    size_t len,
    fz_encoding_format format,
    fz_status *status);

void fz_workbook_set_formulas(
    fz_workbook_h wb,
    const char *sheet,
    uint32_t start_row,
    uint32_t start_col,
    const uint8_t *formulas_payload,
    size_t len,
    fz_encoding_format format,
    fz_status *status);

#ifdef __cplusplus
}
#endif

#endif
