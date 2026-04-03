# Assertions Guide for Schema Registry Adoption Skill

This document explains the quantitative assertions defined for each eval test case. These assertions are objectively verifiable checks that can be automated to grade the skill's output quality.

## Assertion Types

- **file_exists**: Checks if a specific file was created
- **directory_exists**: Checks if a directory was created
- **directory_not_exists**: Checks that a directory was NOT created (for negative tests)
- **file_count**: Checks exact number of files matching a pattern
- **file_count_min**: Checks minimum number of files matching a pattern
- **files_exist**: Checks multiple files exist
- **content_contains**: Checks if file contains a specific pattern (regex)
- **content_contains_all**: Checks if file contains ALL specified patterns
- **grep_count**: Checks exact count of pattern matches
- **grep_count_min**: Checks minimum count of pattern matches

---

## Eval 1: Payment Service (Java Spring Boot with PII)

**Total Assertions: 10**

### Core Deliverables (4 assertions)
1. **schema_report_exists** - Verifies schema-report.md was generated
2. **schemas_directory_exists** - Verifies schemas/ directory created
3. **correct_number_of_schemas** - Checks for 2 schema files (payment-events-value, payment-confirmations-value)
4. **terraform_directory_exists** - Verifies terraform/ directory created

### Terraform Quality (1 assertion)
5. **terraform_files_present** - All 5 core Terraform files exist (providers.tf, variables.tf, schemas.tf, tags.tf, outputs.tf)

### PII Detection (2 assertions)
6. **pii_fields_tagged_email** - customer_email field has PII tag
7. **pii_fields_tagged_credit_card** - credit_card_number field has PII/PRIVATE tags

### Analysis Quality (3 assertions)
8. **report_identifies_category_b** - Correctly categorizes as Category B
9. **report_contains_pii_summary** - PII fields section present with at least 3 fields
10. **upgrade_recommendations_present** - Contains Spring-specific upgrade guidance

---

## Eval 2: Order Processor (Python with auto-registration)

**Total Assertions: 8**

### Risk Detection (3 assertions)
1. **auto_register_risk_detected** - Identifies auto.register.schemas=true as risk
2. **auto_register_locations_found** - Finds both locations (producer.py:11 and config.yaml)
3. **risk_section_in_report** - Report has dedicated RISKS section

### Schema Extraction (1 assertion)
4. **schemas_extracted_from_pydantic** - 2 Avro schemas from Pydantic models

### Terraform Quality (2 assertions)
5. **flagged_auto_register_tf_exists** - flagged-auto-register.tf file created
6. **flagged_schemas_commented_out** - Resources properly commented out

### Analysis Quality (2 assertions)
7. **category_c_identified** - Correctly categorizes as Category C
8. **migration_steps_provided** - Provides steps to disable auto-registration

---

## Eval 3: Ecommerce Platform (Multi-language)

**Total Assertions: 8**

### Multi-Service Detection (3 assertions)
1. **three_services_identified** - Identifies all 3 services
2. **three_languages_detected** - Detects Java, Python, and Node.js
3. **three_schemas_extracted** - Extracts 3+ schemas

### PII & GDPR (2 assertions)
4. **pii_fields_tagged** - At least 5 PII fields tagged
5. **gdpr_compliance_mentioned** - GDPR mentioned in report

### Organization (3 assertions)
6. **consolidated_terraform** - Single Terraform with all 3 schemas
7. **per_service_categorization** - Each service individually categorized
8. **pii_summary_table** - PII summary table or section present

---

## Eval 4: Legacy Event Publisher (Custom serializer)

**Total Assertions: 8**

### Category & Detection (2 assertions)
1. **category_e_identified** - Correctly identifies as Category E
2. **custom_serializer_detected** - Detects json.dumps custom serialization

### Schema Extraction (1 assertion)
3. **schemas_extracted_from_dicts** - 2 schemas from dict structures

### Migration Strategy (4 assertions)
4. **consumers_first_rollout** - Specifies consumers-first ordering
5. **composite_deserializer_mentioned** - Mentions dual-read/composite pattern
6. **breaking_change_warning** - Warns about 5 consumer apps at risk
7. **migration_steps_detailed** - Detailed step-by-step migration plan

### Infrastructure (1 assertion)
8. **terraform_generated** - Terraform for both topics

---

## Eval 5: Streams Processor (Header migration)

**Total Assertions: 7**

### Current State Analysis (2 assertions)
1. **current_sr_integration_detected** - Confirms SR already configured
2. **version_check_mentioned** - Mentions version requirements

### Migration Guidance (3 assertions)
3. **header_serializer_recommended** - Recommends HeaderSchemaIdSerializer
4. **automatic_dual_read_explained** - Explains automatic dual-read behavior
5. **no_consumer_changes_needed** - States consumers auto-support

### Expected Outcomes (2 assertions)
6. **bandwidth_savings_mentioned** - Mentions 5 bytes savings
7. **no_schemas_directory** - No schemas/ created (not needed for this scenario)

---

## Eval 6: Notification Service (.NET with extensive PII)

**Total Assertions: 9**

### PII Tagging (4 assertions)
1. **email_pii_tagged** - Email field tagged
2. **phone_pii_tagged** - Phone field tagged
3. **ssn_private_tagged** - SSN has both PII and PRIVATE tags
4. **at_least_8_pii_fields** - At least 8 PII fields total

### Terraform Data Governance (3 assertions)
5. **terraform_tags_tf_exists** - tags.tf file created
6. **pii_tag_resource_defined** - PII tag resource defined
7. **private_tag_resource_defined** - PRIVATE tag resource defined

### Dependencies (2 assertions)
8. **depends_on_tags** - Schema resources depend on tags
9. **not_using_sr_correctly** - Identifies JsonConvert (not using SR)

---

## Eval 7: Acme Services (Monorepo)

**Total Assertions: 7**

### Service Discovery (2 assertions)
1. **kafka_services_identified** - Finds 3-4 Kafka services
2. **app_catalog_table** - Application catalog table present

### Organization (3 assertions)
3. **per_service_categories** - Each service has category
4. **schemas_extracted** - At least 3 schemas
5. **terraform_with_service_comments** - Terraform has service attribution comments

### Planning (2 assertions)
6. **upgrade_priority_plan** - Prioritized or phased upgrade plan
7. **series_b_readiness** - Mentions governance/Series B readiness

---

## Eval 8: Go Producer (json.Marshal)

**Total Assertions: 8**

### Struct Analysis (2 assertions)
1. **go_structs_analyzed** - Identifies CustomerEvent and OrderEvent structs
2. **schemas_inferred_from_structs** - 2 schemas generated

### Schema Quality (1 assertion)
3. **json_tags_preserved** - Schema fields match struct json tags (snake_case)

### PII Detection (1 assertion)
4. **pii_fields_tagged_go** - At least 4 PII fields tagged

### Go-Specific Guidance (4 assertions)
5. **json_marshal_detected** - Identifies json.Marshal pattern
6. **go_upgrade_instructions** - Go-specific upgrade instructions
7. **dependency_guidance** - Mentions confluent-kafka-go dependency
8. **sensitive_data_identified** - Acknowledges sensitive customer data

---

## Eval 9: Transaction System (Multi-schema topic)

**Total Assertions: 8**

### Problem Detection (2 assertions)
1. **multi_schema_topic_detected** - Identifies multi-schema topic
2. **two_event_types_identified** - Identifies both event types

### Schema Generation (3 assertions)
3. **individual_schemas_extracted** - Individual event schemas created
4. **wrapper_schema_generated** - Wrapper/envelope schema created
5. **union_or_oneof_used** - Uses union (Avro) or oneOf (JSON Schema)

### Terraform Quality (1 assertion)
6. **schema_references_in_terraform** - Has schema_reference blocks

### Documentation (2 assertions)
7. **cross_reference_table** - Cross-reference table showing events→topics
8. **wrapper_pattern_explained** - Explains wrapper schema pattern

---

## Eval 10: API Gateway (Negative test - No Kafka)

**Total Assertions: 7**

### Negative Detection (4 assertions)
1. **no_kafka_dependencies_found** - States no Kafka found
2. **no_producers_found** - Confirms 0 producers
3. **no_consumers_found** - Confirms 0 consumers
4. **no_action_needed** - Recommends no SR action

### Correct Non-Generation (2 assertions)
5. **no_schemas_generated** - No schemas/ directory created
6. **no_terraform_generated** - No terraform/ directory created

### Alternative Technology (1 assertion)
7. **express_axios_only** - Identifies express/axios (HTTP/REST stack)

---

## Summary Statistics

- **Total Assertions**: 80 across 10 evals
- **Average per Eval**: 8 assertions
- **Most Assertions**: Eval 1 (10), Eval 6 (9)
- **Fewest Assertions**: Eval 5, Eval 10 (7 each)

## Assertion Categories

| Category | Count | Description |
|----------|-------|-------------|
| File Existence | 15 | Checks core deliverables created |
| PII Detection | 12 | Verifies PII field tagging |
| Content Quality | 28 | Checks report content and recommendations |
| Terraform Quality | 10 | Validates Terraform structure |
| Category Detection | 8 | Verifies correct categorization |
| Negative Tests | 7 | Checks what should NOT be created |

## Using These Assertions

These assertions will be used to:
1. **Grade outputs** - Automatically score each eval run
2. **Compare with-skill vs baseline** - See which performs better
3. **Track improvements** - Measure skill iteration quality
4. **Identify regressions** - Catch when quality decreases

Each assertion can be checked programmatically and returns pass/fail with evidence.
