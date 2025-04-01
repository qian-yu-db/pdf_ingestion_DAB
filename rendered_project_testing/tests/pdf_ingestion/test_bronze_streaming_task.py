from pdf_ingestion.bronze_streaming_task import parse_args
import sys

def test_parse_args_valid_inputs(monkeypatch):
    test_args = [
        "script_name",  # typically argv[0]
        "--catalog", "my_catalog",
        "--schema", "my_schema",
        "--volume", "my_volume",
        "--checkpoints_volume", "my_checkpoints",
        "--table_prefix", "my_prefix",
        "--reset_data", "true"
    ]

    monkeypatch.setattr(sys, "argv", test_args)

    args = parse_args()

    assert args.catalog == "my_catalog"
    assert args.schema == "my_schema"
    assert args.volume == "my_volume"
    assert args.checkpoints_volume == "my_checkpoints"
    assert args.table_prefix == "my_prefix"
    assert args.reset_data is True


def test_parse_args_reset_data_false_by_default(monkeypatch):
    # skip --reset_data to see if default is "false"
    test_args = [
        "script_name",
        "--catalog", "cat",
        "--schema", "sch",
        "--volume", "vol",
        "--checkpoints_volume", "cp_vol",
        "--table_prefix", "prefix"
    ]

    monkeypatch.setattr(sys, "argv", test_args)
    args = parse_args()
    assert args.reset_data is False
