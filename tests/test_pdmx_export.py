import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))
from omr_lab.data.pdmx_export import export_pdmx_to_musicxml


def test_pdmx_export_basic(tmp_path: Path) -> None:
    pdmx_root = tmp_path / "pdmx"
    data_dir = pdmx_root / "data"
    data_dir.mkdir(parents=True)

    sample = {
        "tracks": [
            {
                "notes": [{"pitch": 60, "time": 0, "duration": 480}],
                "lyrics": [{"time": 0, "text": "la"}],
            }
        ]
    }
    src_json = data_dir / "sample.json"
    src_json.write_text(json.dumps(sample), encoding="utf-8")

    out_dir = tmp_path / "out"
    summary = export_pdmx_to_musicxml(pdmx_root, out_dir, jobs=1, ext="musicxml")

    assert summary == {"exported": 1, "failed": 0, "total": 1}

    xml_path = out_dir / "data" / "sample.musicxml"
    assert xml_path.exists()
    xml_text = xml_path.read_text(encoding="utf-8")
    assert "<note" in xml_text
    assert "TODO" not in xml_text
