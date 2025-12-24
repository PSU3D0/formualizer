#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "formualizer_cffi.h"

static void fail_status(const char *context, fz_status status) {
    if (status.code == FZ_STATUS_OK) {
        return;
    }
    if (status.error.data != NULL && status.error.len > 0) {
        fprintf(stderr, "%s failed: %.*s\n", context, (int)status.error.len, status.error.data);
        fz_buffer_free(status.error);
    } else {
        fprintf(stderr, "%s failed with unknown error\n", context);
    }
    exit(1);
}

static char *buffer_to_string(fz_buffer buffer) {
    char *out = (char *)malloc(buffer.len + 1);
    if (!out) {
        return NULL;
    }
    if (buffer.len > 0) {
        memcpy(out, buffer.data, buffer.len);
    }
    out[buffer.len] = '\0';
    return out;
}

static void assert_contains(const char *haystack, const char *needle, const char *context) {
    if (!strstr(haystack, needle)) {
        fprintf(stderr, "%s: expected substring '%s'\n", context, needle);
        exit(1);
    }
}

int main(void) {
    fz_status status = {0};

    if (fz_common_abi_version() != 1 || fz_parse_abi_version() != 1 || fz_workbook_abi_version() != 1) {
        fprintf(stderr, "unexpected ABI versions\n");
        return 1;
    }

    const char *xlsx_path = "/tmp/formualizer_cffi_smoke.xlsx";
    fz_workbook_h wb = fz_workbook_open_xlsx(xlsx_path, &status);
    fail_status("fz_workbook_open_xlsx", status);

    fz_workbook_add_sheet(wb, "Sheet2", &status);
    fail_status("fz_workbook_add_sheet Sheet2", status);

    const char *a1_json = "{\"Number\":12.0}";
    fz_workbook_set_cell_value(
        wb,
        "Sheet1",
        1,
        1,
        (const uint8_t *)a1_json,
        strlen(a1_json),
        FZ_ENCODING_JSON,
        &status);
    fail_status("fz_workbook_set_cell_value", status);

    const char *targets_json = "[{\"sheet\":\"Sheet1\",\"row\":1,\"col\":2}]";
    fz_buffer eval_buffer = fz_workbook_evaluate_cells(
        wb,
        (const uint8_t *)targets_json,
        strlen(targets_json),
        FZ_ENCODING_JSON,
        &status);
    fail_status("fz_workbook_evaluate_cells", status);

    char *eval_json = buffer_to_string(eval_buffer);
    if (!eval_json) {
        fprintf(stderr, "allocation failure\n");
        fz_buffer_free(eval_buffer);
        return 1;
    }
    assert_contains(eval_json, "24", "eval result");
    free(eval_json);
    fz_buffer_free(eval_buffer);

    fz_buffer eval_all_buffer = fz_workbook_evaluate_all(wb, FZ_ENCODING_JSON, &status);
    fail_status("fz_workbook_evaluate_all", status);

    char *eval_all_json = buffer_to_string(eval_all_buffer);
    if (!eval_all_json) {
        fprintf(stderr, "allocation failure\n");
        fz_buffer_free(eval_all_buffer);
        return 1;
    }
    assert_contains(eval_all_json, "\"cycle_errors\":0", "eval all result");
    free(eval_all_json);
    fz_buffer_free(eval_all_buffer);

    fz_buffer value_buffer = fz_workbook_get_cell_value(
        wb,
        "Sheet1",
        1,
        2,
        FZ_ENCODING_JSON,
        &status);
    fail_status("fz_workbook_get_cell_value", status);

    char *value_json = buffer_to_string(value_buffer);
    if (!value_json) {
        fprintf(stderr, "allocation failure\n");
        fz_buffer_free(value_buffer);
        return 1;
    }
    assert_contains(value_json, "24", "cell value");
    free(value_json);
    fz_buffer_free(value_buffer);

    int has_sheet = fz_workbook_has_sheet(wb, "Sheet2", &status);
    fail_status("fz_workbook_has_sheet", status);
    if (has_sheet != 1) {
        fprintf(stderr, "expected Sheet2 to exist\n");
        return 1;
    }

    fz_buffer names_buffer = fz_workbook_sheet_names(wb, FZ_ENCODING_JSON, &status);
    fail_status("fz_workbook_sheet_names", status);
    char *names_json = buffer_to_string(names_buffer);
    if (!names_json) {
        fprintf(stderr, "allocation failure\n");
        fz_buffer_free(names_buffer);
        return 1;
    }
    assert_contains(names_json, "Sheet1", "sheet names");
    free(names_json);
    fz_buffer_free(names_buffer);

    fz_buffer dims_buffer = fz_workbook_sheet_dimensions(wb, "Sheet1", FZ_ENCODING_JSON, &status);
    fail_status("fz_workbook_sheet_dimensions", status);
    char *dims_json = buffer_to_string(dims_buffer);
    if (!dims_json) {
        fprintf(stderr, "allocation failure\n");
        fz_buffer_free(dims_buffer);
        return 1;
    }
    assert_contains(dims_json, "\"rows\"", "sheet dimensions");
    assert_contains(dims_json, "\"cols\"", "sheet dimensions");
    free(dims_json);
    fz_buffer_free(dims_buffer);

    const char *values_json =
        "[[{\"Number\":1.0},{\"Number\":2.0}],[{\"Text\":\"Hi\"},{\"Boolean\":true}]]";
    fz_workbook_set_values(
        wb,
        "Sheet1",
        2,
        1,
        (const uint8_t *)values_json,
        strlen(values_json),
        FZ_ENCODING_JSON,
        &status);
    fail_status("fz_workbook_set_values", status);

    const char *range_json =
        "{\"sheet\":\"Sheet1\",\"start_row\":2,\"start_col\":1,\"end_row\":3,\"end_col\":2}";
    fz_buffer range_buffer = fz_workbook_read_range(
        wb,
        (const uint8_t *)range_json,
        strlen(range_json),
        FZ_ENCODING_JSON,
        &status);
    fail_status("fz_workbook_read_range", status);

    char *range_data = buffer_to_string(range_buffer);
    if (!range_data) {
        fprintf(stderr, "allocation failure\n");
        fz_buffer_free(range_buffer);
        return 1;
    }
    assert_contains(range_data, "Hi", "range read");
    free(range_data);
    fz_buffer_free(range_buffer);

    fz_workbook_free(wb);
    printf("cffi_smoke: ok\n");
    return 0;
}
