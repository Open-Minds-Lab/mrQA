
from mrQA import check_compliance

from mrQA import run_parallel

dummy_DS = []

def test_equivalence_seq_vs_parallel():
    dataset = import_dataset(data_root=args.data_root,
                             style=args.style,
                             name=args.name,
                             reindex=args.reindex,
                             verbose=args.verbose,
                             include_phantom=args.include_phantom,
                             metadata_root=args.metadata_root)
    check_compliance(dataset=dataset,
                     strategy=args.strategy,
                     output_dir=args.output_dir,
                     reference_path=args.reference_path)
