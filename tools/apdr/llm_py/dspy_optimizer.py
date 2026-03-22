"""DSPy MIPROv2 prompt optimization for APDR import resolution.

Usage:
    python -m llm_py.dspy_optimizer --train-data data/seed/reference_aliases.tsv \
        --model ollama_chat/gemma3:12b --output optimized_prompts.json

This script:
1. Loads import→package ground truth from seed TSV files
2. Defines a DSPy signature for import resolution
3. Runs MIPROv2 Bayesian optimization to find optimal:
   - System prompt instructions
   - Few-shot examples (selected from training data)
4. Exports the optimized prompt configuration as JSON
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sys
from pathlib import Path

logger = logging.getLogger("apdr_llm.dspy_optimizer")


def load_training_data(tsv_paths: list[str]) -> list[dict]:
    """Load import→package pairs from TSV files as training examples."""
    examples = []
    seen = set()
    for path_str in tsv_paths:
        path = Path(path_str)
        if not path.exists():
            logger.warning("TSV file not found: %s", path)
            continue
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) >= 2:
                import_name = parts[0].strip()
                package_name = parts[1].strip()
                # Only include non-identity mappings for training
                norm_i = import_name.lower().replace("-", "_")
                norm_p = package_name.lower().replace("-", "_")
                if norm_i != norm_p and import_name not in seen:
                    examples.append({
                        "import_name": import_name,
                        "package_name": package_name,
                    })
                    seen.add(import_name)
    return examples


def run_optimization(
    train_data: list[dict],
    model: str = "ollama_chat/gemma3:12b",
    api_base: str = "http://localhost:11434",
    num_trials: int = 20,
    max_bootstrapped_demos: int = 5,
    max_labeled_demos: int = 8,
) -> dict:
    """Run MIPROv2 optimization on import resolution task.

    Returns optimized prompt configuration as a dict.
    """
    try:
        import dspy
    except ImportError:
        logger.error("dspy not installed. Run: pip install dspy")
        return {"error": "dspy not installed"}

    # Configure DSPy with LiteLLM backend
    lm = dspy.LM(model, api_base=api_base, temperature=0.0, max_tokens=256)
    dspy.configure(lm=lm)

    # Define the signature
    class ImportToPackage(dspy.Signature):
        """Given a Python import name, return the correct PyPI package name.
        Consider common patterns: prefix additions (python-, django-),
        completely different names (cv2->opencv-python), and whether the
        import is a standard library module (skip those)."""

        import_name: str = dspy.InputField(desc="Python import name to resolve")
        package_name: str = dspy.OutputField(desc="Correct PyPI package name")

    # Build DSPy examples
    trainset = []
    for ex in train_data:
        trainset.append(
            dspy.Example(
                import_name=ex["import_name"],
                package_name=ex["package_name"],
            ).with_inputs("import_name")
        )

    # Split into train/val
    random.shuffle(trainset)
    split = max(1, int(len(trainset) * 0.8))
    train_split = trainset[:split]
    val_split = trainset[split:]

    if not val_split:
        val_split = train_split[:10]

    # Define the module
    class ImportResolver(dspy.Module):
        def __init__(self):
            super().__init__()
            self.predict = dspy.ChainOfThought(ImportToPackage)

        def forward(self, import_name: str):
            return self.predict(import_name=import_name)

    # Define metric
    def exact_match(example, prediction, trace=None):
        pred_pkg = prediction.package_name.strip().lower().replace("-", "_")
        gold_pkg = example.package_name.strip().lower().replace("-", "_")
        return pred_pkg == gold_pkg

    # Run MIPROv2
    logger.info(
        "Starting MIPROv2 with %d train, %d val examples, %d trials",
        len(train_split), len(val_split), num_trials,
    )

    try:
        optimizer = dspy.MIPROv2(
            metric=exact_match,
            num_threads=1,
            max_bootstrapped_demos=max_bootstrapped_demos,
            max_labeled_demos=max_labeled_demos,
        )

        optimized = optimizer.compile(
            ImportResolver(),
            trainset=train_split,
            valset=val_split,
            num_trials=num_trials,
            minibatch=True,
            minibatch_size=min(25, len(val_split)),
        )

        # Extract the optimized prompt
        result = {
            "optimized": True,
            "num_train": len(train_split),
            "num_val": len(val_split),
            "num_trials": num_trials,
        }

        # Save the compiled module
        try:
            state = optimized.save("optimized_resolver.json")
            result["module_path"] = "optimized_resolver.json"
        except Exception as e:
            logger.warning("Could not save module: %s", e)

        # Extract demos for export
        if hasattr(optimized, "predict") and hasattr(optimized.predict, "demos"):
            demos = []
            for demo in optimized.predict.demos:
                demos.append({
                    "import_name": getattr(demo, "import_name", ""),
                    "package_name": getattr(demo, "package_name", ""),
                    "reasoning": getattr(demo, "reasoning", ""),
                })
            result["few_shot_examples"] = demos

        return result

    except Exception as e:
        logger.error("MIPROv2 optimization failed: %s", e)
        return {"error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="DSPy MIPROv2 prompt optimizer for APDR")
    parser.add_argument(
        "--train-data", nargs="+", required=True,
        help="TSV files with import→package ground truth",
    )
    parser.add_argument("--model", default="ollama_chat/gemma3:12b")
    parser.add_argument("--api-base", default="http://localhost:11434")
    parser.add_argument("--num-trials", type=int, default=20)
    parser.add_argument("--output", default="optimized_prompts.json")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    train_data = load_training_data(args.train_data)
    logger.info("Loaded %d non-identity training examples", len(train_data))

    if len(train_data) < 10:
        logger.error("Not enough training examples (need at least 10)")
        sys.exit(1)

    result = run_optimization(
        train_data=train_data,
        model=args.model,
        api_base=args.api_base,
        num_trials=args.num_trials,
    )

    Path(args.output).write_text(json.dumps(result, indent=2))
    logger.info("Saved optimized prompts to %s", args.output)


if __name__ == "__main__":
    main()
