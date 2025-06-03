#!/usr/bin/env python3

from sqlflow.parser.parser import Parser


def test_pipeline_parsing():
    parser = Parser()

    # Read the pipeline file
    with open("/app/pipelines/06_enhanced_s3_connector_demo.sf", "r") as f:
        text = f.read()

    print("First 500 chars:")
    print(text[:500])
    print()

    print("--- Parsing ---")
    try:
        pipeline = parser.parse(text)
        print(f"Parsed {len(pipeline.steps)} steps")

        # Check first source step
        first_source = None
        for step in pipeline.steps:
            if hasattr(step, "connector_type") and step.connector_type == "S3":
                first_source = step
                break

        if first_source:
            print(f"First S3 source: {first_source.name}")
            print(f"Params: {first_source.params}")
            print(f"Line: {first_source.line_number}")

    except Exception as e:
        print(f"Parse error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_pipeline_parsing()
