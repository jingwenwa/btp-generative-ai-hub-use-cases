CLASS <<AMDP_NAME>> DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    INTERFACES if_hemi_model_management.
    INTERFACES if_hemi_procedure.
    INTERFACES if_amdp_marker_hdb.

    TYPES:
      BEGIN OF ty_train_input,
<<TRAIN_INPUT_STRUCTURE>>
      END OF ty_train_input,
      tt_training_data TYPE STANDARD TABLE OF ty_train_input WITH DEFAULT KEY,
      tt_predict_data  TYPE STANDARD TABLE OF ty_train_input WITH DEFAULT KEY,
      <<CAST_TARGET_OUTPUT>>
      BEGIN OF ty_predict_result,
<<RESULT_OUTPUT_STRUCTURE>>
      END OF ty_predict_result,
      tt_predict_result TYPE STANDARD TABLE OF ty_predict_result WITH DEFAULT KEY.

    TYPES:
      BEGIN OF ty_metrics,
        key   TYPE string,
        value TYPE string,
      END OF ty_metrics,
      tt_metrics TYPE STANDARD TABLE OF ty_metrics WITH DEFAULT KEY,
      BEGIN OF ty_model,
        row_index     TYPE int4,
        model_content TYPE string,
      END OF ty_model,
      tt_model TYPE STANDARD TABLE OF ty_model WITH DEFAULT KEY.

    CLASS-METHODS training
      AMDP OPTIONS READ-ONLY
      IMPORTING
        VALUE(it_data)                TYPE tt_training_data
        VALUE(it_param)               TYPE if_hemi_model_management=>tt_pal_param
      EXPORTING
        VALUE(et_model)               TYPE tt_model
        VALUE(et_metrics)             TYPE tt_metrics
      RAISING
        cx_amdp_execution_failed.

    CLASS-METHODS predict_with_model_version
      AMDP OPTIONS READ-ONLY
      IMPORTING
        VALUE(it_data)   TYPE tt_predict_data
        VALUE(it_model)  TYPE tt_model
        VALUE(it_param)  TYPE if_hemi_model_management=>tt_pal_param
      EXPORTING
        VALUE(et_result) TYPE tt_predict_result
      RAISING
        cx_amdp_execution_failed.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.

CLASS <<AMDP_NAME>> IMPLEMENTATION.

  METHOD if_hemi_model_management~get_amdp_class_name.
    DATA lr_self TYPE REF TO <<AMDP_NAME>>.
    TRY.
        CREATE OBJECT lr_self.
        ev_name = cl_abap_classdescr=>get_class_name( lr_self ).
      CATCH cx_badi_context_error.
      CATCH cx_badi_not_implemented.
    ENDTRY.
  ENDMETHOD.

  METHOD if_hemi_procedure~get_procedure_parameters.
    et_training = VALUE #(
       ( name = 'IT_DATA'                role = cl_hemi_constants=>cs_proc_role-data                         )
       ( name = 'IT_PARAM'               role = cl_hemi_constants=>cs_proc_role-param                        )
       ( name = 'ET_MODEL'               role = cl_hemi_constants=>cs_proc_role-model                        )
       ( name = 'ET_METRICS'             role = cl_hemi_constants=>cs_proc_role-stats add_info = 'metrics'   )
    ).
    et_apply = VALUE #(
       ( name = 'IT_DATA'   role = cl_hemi_constants=>cs_proc_role-data                        )
       ( name = 'IT_MODEL'  role = cl_hemi_constants=>cs_proc_role-model add_info = 'et_model' )
       ( name = 'IT_PARAM'  role = cl_hemi_constants=>cs_proc_role-param                       )
       ( name = 'ET_RESULT' role = cl_hemi_constants=>cs_proc_role-result                      )
    ).
  ENDMETHOD.

  METHOD if_hemi_model_management~get_meta_data.
    es_meta_data-model_parameters = <<PARAMETER>>.
    es_meta_data-model_parameter_defaults = <<PARAMETER_DEFAULT>>.

    es_meta_data-training_data_set = '<<TRAINING_DATASET>>'.
    es_meta_data-apply_data_set    = '<<APPLY_DATASET>>'.

    es_meta_data-field_descriptions = VALUE #( ( name = '<<TARGET_COLUMN>>' role = cl_hemi_constants=>cs_field_role-target )<<KEY_FIELD_DESCRIPTION>> ).
  ENDMETHOD.

  METHOD training BY DATABASE PROCEDURE FOR HDB LANGUAGE SQLSCRIPT OPTIONS READ-ONLY.
    call _sys_afl.pal_pipeline_fit(:it_data, :it_param, lt_model, lt_info);
    et_model = select * from :lt_model;
    et_metrics = select * from :lt_info;
  ENDMETHOD.

  METHOD predict_with_model_version BY DATABASE PROCEDURE FOR HDB LANGUAGE SQLSCRIPT OPTIONS READ-ONLY.
    lt_data = select
<<PREDICT_DATA_COLS>>
              from :it_data;
    call _sys_afl.pal_pipeline_predict(:lt_data, :it_model, :it_pram, lt_result, lt_stats);
    et_result = select * from lt_result;
  ENDMETHOD.

ENDCLASS.
