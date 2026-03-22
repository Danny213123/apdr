"""LoRA fine-tuning for APDR import-to-package resolution.

Usage:
    python -m llm_py.finetune.train \
        --base-model unsloth/gemma-2-9b-it-bnb-4bit \
        --train-data data/seed/reference_aliases.tsv data/seed/name_discrepancies.tsv \
        --output-dir ./finetuned-apdr \
        --export-gguf q4_k_m

This script:
1. Loads import→package ground truth from seed TSV files
2. Formats as chat-template training examples
3. Runs QLoRA fine-tuning with Unsloth (4-bit quantized)
4. Exports as GGUF for use with Ollama

After training, create an Ollama Modelfile:
    FROM ./finetuned-apdr/model-q4_k_m.gguf
    TEMPLATE "{{ .System }}\\n{{ .Prompt }}"
    PARAMETER temperature 0
    SYSTEM "You resolve Python imports to PyPI package names."

Then: ollama create apdr-resolver -f Modelfile
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sys
from pathlib import Path

logger = logging.getLogger("apdr_llm.finetune")

SYSTEM_PROMPT = (
    "You are resolving Python imports to PyPI package names. "
    "Given an import name, return the correct PyPI package name. "
    "If the import name is the same as the package name (after normalizing "
    "hyphens/underscores), return it as-is. Common patterns: "
    "cv2->opencv-python, PIL->Pillow, yaml->PyYAML, sklearn->scikit-learn, "
    "bs4->beautifulsoup4, dateutil->python-dateutil."
)


def load_training_pairs(tsv_paths: list[str]) -> list[dict]:
    """Load import→package pairs from TSV files."""
    pairs = []
    seen = set()
    for path_str in tsv_paths:
        path = Path(path_str)
        if not path.exists():
            logger.warning("TSV not found: %s", path)
            continue
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) >= 2:
                import_name = parts[0].strip()
                package_name = parts[1].strip()
                if import_name and package_name and import_name not in seen:
                    pairs.append({
                        "import_name": import_name,
                        "package_name": package_name,
                    })
                    seen.add(import_name)
    return pairs


def format_chat_examples(pairs: list[dict]) -> list[dict]:
    """Format training pairs as chat-template conversations."""
    examples = []
    for pair in pairs:
        conversation = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    f"What PyPI package provides the Python import `{pair['import_name']}`? "
                    f"Return JSON: {{\"import_name\": \"...\", \"package_name\": \"...\"}}"
                ),
            },
            {
                "role": "assistant",
                "content": json.dumps({
                    "import_name": pair["import_name"],
                    "package_name": pair["package_name"],
                }),
            },
        ]
        examples.append({"conversations": conversation})
    return examples


def train(
    base_model: str,
    train_data: list[dict],
    output_dir: str,
    epochs: int = 3,
    lr: float = 2e-4,
    batch_size: int = 4,
    max_seq_length: int = 512,
    lora_r: int = 16,
    lora_alpha: int = 32,
    export_gguf: str = "",
) -> dict:
    """Run QLoRA fine-tuning with Unsloth."""
    try:
        from unsloth import FastLanguageModel
        from unsloth.chat_templates import get_chat_template
        from trl import SFTTrainer
        from transformers import TrainingArguments
        from datasets import Dataset
    except ImportError as e:
        logger.error(
            "Required packages not installed. Run:\n"
            "  pip install unsloth trl transformers datasets\n"
            "Error: %s", e
        )
        return {"error": str(e)}

    logger.info("Loading base model: %s", base_model)
    model, tokenizer = FastLanguageModel.from_pretrained(
        model_name=base_model,
        max_seq_length=max_seq_length,
        load_in_4bit=True,
    )

    # Apply LoRA adapters
    model = FastLanguageModel.get_peft_model(
        model,
        r=lora_r,
        lora_alpha=lora_alpha,
        lora_dropout=0.05,
        target_modules=[
            "q_proj", "k_proj", "v_proj", "o_proj",
            "gate_proj", "up_proj", "down_proj",
        ],
        bias="none",
        use_gradient_checkpointing="unsloth",
    )

    # Apply chat template
    tokenizer = get_chat_template(tokenizer, chat_template="gemma")

    # Format training data
    chat_examples = format_chat_examples(train_data)
    random.shuffle(chat_examples)

    def format_for_training(example):
        convos = example["conversations"]
        text = tokenizer.apply_chat_template(
            convos, tokenize=False, add_generation_prompt=False,
        )
        return {"text": text}

    dataset = Dataset.from_list(chat_examples)
    dataset = dataset.map(format_for_training)

    logger.info("Training on %d examples for %d epochs", len(dataset), epochs)

    # Training
    trainer = SFTTrainer(
        model=model,
        tokenizer=tokenizer,
        train_dataset=dataset,
        dataset_text_field="text",
        max_seq_length=max_seq_length,
        args=TrainingArguments(
            output_dir=output_dir,
            per_device_train_batch_size=batch_size,
            gradient_accumulation_steps=4,
            warmup_steps=10,
            num_train_epochs=epochs,
            learning_rate=lr,
            fp16=True,
            logging_steps=10,
            save_strategy="epoch",
            optim="adamw_8bit",
            seed=42,
        ),
    )

    trainer.train()

    # Save LoRA adapter
    adapter_path = Path(output_dir) / "lora-adapter"
    model.save_pretrained(str(adapter_path))
    tokenizer.save_pretrained(str(adapter_path))
    logger.info("LoRA adapter saved to %s", adapter_path)

    result = {
        "adapter_path": str(adapter_path),
        "num_examples": len(dataset),
        "epochs": epochs,
    }

    # Export GGUF for Ollama
    if export_gguf:
        try:
            gguf_path = Path(output_dir) / f"model-{export_gguf}.gguf"
            model.save_pretrained_gguf(
                str(gguf_path.parent),
                tokenizer,
                quantization_method=export_gguf,
            )
            result["gguf_path"] = str(gguf_path)
            logger.info("GGUF exported: %s", gguf_path)

            # Generate Modelfile for Ollama
            modelfile = (
                f"FROM ./{gguf_path.name}\n"
                f'TEMPLATE "{{{{ .System }}}}\\n{{{{ .Prompt }}}}"\n'
                f"PARAMETER temperature 0\n"
                f'SYSTEM "{SYSTEM_PROMPT}"\n'
            )
            modelfile_path = Path(output_dir) / "Modelfile"
            modelfile_path.write_text(modelfile)
            result["modelfile_path"] = str(modelfile_path)
            logger.info("Modelfile written: %s", modelfile_path)
        except Exception as e:
            logger.warning("GGUF export failed: %s", e)
            result["gguf_error"] = str(e)

    return result


def main():
    parser = argparse.ArgumentParser(description="LoRA fine-tuning for APDR")
    parser.add_argument(
        "--base-model",
        default="unsloth/gemma-2-9b-it-bnb-4bit",
        help="Base model for fine-tuning",
    )
    parser.add_argument(
        "--train-data", nargs="+", required=True,
        help="TSV files with import→package ground truth",
    )
    parser.add_argument("--output-dir", default="./finetuned-apdr")
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--lr", type=float, default=2e-4)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument(
        "--export-gguf", default="q4_k_m",
        help="GGUF quantization method (q4_k_m, q5_k_m, q8_0, f16). Empty to skip.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    pairs = load_training_pairs(args.train_data)
    logger.info("Loaded %d training pairs", len(pairs))

    if len(pairs) < 10:
        logger.error("Need at least 10 training pairs")
        sys.exit(1)

    result = train(
        base_model=args.base_model,
        train_data=pairs,
        output_dir=args.output_dir,
        epochs=args.epochs,
        lr=args.lr,
        batch_size=args.batch_size,
        export_gguf=args.export_gguf,
    )

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
