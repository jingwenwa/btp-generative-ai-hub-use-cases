CLASS <<AMDP_NAME>> DEFINITION
  PUBLIC
  INHERITING FROM cl_hemi_model_mgmt_ucl_base
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    INTERFACES if_hemi_model_management.

    TYPES:
      BEGIN OF ts_data,
<<TRAIN_INPUT_STRUCTURE>>
      END OF ts_data,
      tt_training_data TYPE STANDARD TABLE OF ts_data WITH DEFAULT KEY,
      tt_predict_data  TYPE STANDARD TABLE OF ts_data WITH DEFAULT KEY,
      BEGIN OF ty_predict_result,
<<RESULT_OUTPUT_STRUCTURE>>
<<REASON_CODE_STRUCTURE>>
      END OF ty_predict_result,
      tt_predict_result TYPE STANDARD TABLE OF ty_predict_result WITH DEFAULT KEY.

    CLASS-METHODS training
      IMPORTING
        VALUE(it_data)                TYPE tt_training_data
        VALUE(it_param)               TYPE if_hemi_model_management=>tt_pal_param
      EXPORTING
        VALUE(et_model)               TYPE cl_hemi_model_mgmt_ucl_base=>tt_model
        VALUE(et_confusion_matrix)    TYPE shemi_confusion_matrix_t
        VALUE(et_variable_importance) TYPE shemi_variable_importance_t
        VALUE(et_metrics)             TYPE if_hemi_model_management=>tt_metrics
        VALUE(et_gen_info)            TYPE if_hemi_model_management=>tt_metrics
      RAISING
        cx_amdp_execution_failed.

    CLASS-METHODS predict_with_model_version
      IMPORTING
        VALUE(it_data)   TYPE tt_predict_data
        VALUE(it_model)  TYPE cl_hemi_model_mgmt_ucl_base=>tt_model
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

  METHOD if_hemi_model_management~get_meta_data.
    es_meta_data-model_parameters = <<PARAMETER>>.
    es_meta_data-model_parameter_defaults = <<PARAMETER_DEFAULT>>.

    es_meta_data-training_data_set = '<<TRAINING_DATASET>>'.
    es_meta_data-apply_data_set    = '<<APPLY_DATASET>>'.

    es_meta_data-field_descriptions = VALUE #( ( name = '<<TARGET_COLUMN>>' role = cl_hemi_constants=>cs_field_role-target ) ).
  ENDMETHOD.

  METHOD training BY DATABASE PROCEDURE FOR HDB LANGUAGE SQLSCRIPT OPTIONS READ-ONLY.
    declare validation_ki double;
    declare train_ki      double;
    /* example https://github.com/SAP-samples/hana-ml-samples/blob/main/PAL-AMDP/ISLM-UnifiedClassification-example/zcl_islm_pal_ucl_sflight.abap
       provides more information on training and prediction */

    /* Step 1. Execute training */

    call _sys_afl.pal_unified_classification(:it_data, :it_param, :et_model, :et_variable_importance, lt_stat, lt_opt, lt_cm, lt_metrics, lt_ph1, lt_ph2);


    /* Step 2. Compute metrics */

    -- output confusion matrix
    et_confusion_matrix = select * from :lt_cm;

    -- simplified calculation of KI (predictive power, quality indicator of a classification) and KR (predicition confidence, robustness indicator of a model)
    select to_double(stat_value) * 2 - 1 into validation_ki from :lt_stat where stat_name = 'AUC';
    select to_double(stat_value) * 2 - 1 into train_ki      from :lt_stat where stat_name = 'AUC';

    et_metrics = select 'PredictivePower'      as key, to_nvarchar(:validation_ki)                        as value from dummy
       union all select 'PredictionConfidence' as key, to_nvarchar(1.0 - abs(:validation_ki - :train_ki)) as value from dummy
    -- Provide metrics that are displayed in the quality information section of a model version in the ISLM Intelligent Scenario Management app
    /* <<<<<< TODO: Starting point of adaptation */
       union all select 'AUC' as key, stat_value as value from :lt_stat where stat_name = 'AUC';
    /* <<<<<< TODO: End point of adaptation */

    -- Provide metrics that are displayed in the general additional info section of a model version in the ISLM Intelligent Scenario Management app
    et_gen_info =
    /* <<<<<< TODO: Starting point of adaptation */
        select stat_name as key, stat_value as value from :lt_stat where class_name is null;
    /* <<<<<< TODO: End point of adaptation */
  ENDMETHOD.

  METHOD predict_with_model_version BY DATABASE PROCEDURE FOR HDB LANGUAGE SQLSCRIPT OPTIONS READ-ONLY.

    /* Step 1. Input data preprocessing (missing values, rescaling, encoding, etc).
       Note: the input data preprocessing must correspond with the one in the training method.
       Based on the scenario, add ids, select fields relevant for the training, cast to the appropriate data type, convert nulls into meaningful values.
       Note: decimal must be converted into double. */
    lt_data = select
<<PREDICT_DATA_COLS>>
              from :it_data;

    /* Step 2. Execute prediction */

    call _sys_afl.pal_unified_classification_predict(:lt_data, :it_model, :it_param, lt_result, lt_placeholder2);

    /* Step 3. Map prediction results back to the composite key */

    et_result = select
<<RESULT_FIELDS>>
<<RESULT_REASON_CODE_FIELDS>>
                from :lt_result as result;
  ENDMETHOD.

ENDCLASS.
