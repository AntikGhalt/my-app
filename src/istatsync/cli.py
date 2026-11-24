"""Command line entry point: `istatsync run | list | describe`."""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import replace

from .catalog import load_catalog
from .runner import Runner
from .sdmx import SOURCES, SdmxClient
from .settings import Settings


def _clients(settings: Settings, cache) -> dict[str, SdmxClient]:
    istat = replace(
        SOURCES["istat"],
        base_url=settings.base_url,
        min_interval=settings.min_request_interval,
    )
    sources = {**SOURCES, "istat": istat}
    return {
        name: SdmxClient(
            source,
            cache=cache,
            timeout=settings.request_timeout,
            max_retries=settings.max_retries,
            structure_ttl_days=settings.structure_ttl_days,
        )
        for name, source in sources.items()
    }


def _sink(settings: Settings, local: bool):
    if settings.drive_folder_id and not local:
        from .sinks.drive import DriveSink

        return DriveSink(settings.drive_folder_id)
    from .sinks import LocalSink

    return LocalSink(settings.output_dir)


def cmd_run(args, settings: Settings) -> int:
    datasets = load_catalog(settings.catalog_path)
    if args.only:
        wanted = set(args.only)
        missing = wanted - {d.id for d in datasets}
        if missing:
            print(f"unknown dataset(s): {', '.join(sorted(missing))}", file=sys.stderr)
            return 2
        datasets = [d for d in datasets if d.id in wanted]
    else:
        datasets = [d for d in datasets if d.enabled]
    if args.frequency:
        datasets = [d for d in datasets if d.frequency == args.frequency]

    sink = _sink(settings, args.local)
    logging.info("writing to %s", sink.describe())
    clients = _clients(settings, cache=sink)
    outcomes = Runner(clients, sink, settings.timezone, force=args.force).run(datasets)

    width = max((len(o.dataset) for o in outcomes), default=10)
    for o in outcomes:
        print(f"{o.dataset:<{width}}  {o.status:<9}  {o.detail}")
    for name, client in clients.items():
        if client.requests_made:
            print(f"{client.requests_made} requests to {name}")
    return 1 if any(o.status in ("failed", "skipped") for o in outcomes) else 0


def cmd_list(args, settings: Settings) -> int:
    for ds in load_catalog(settings.catalog_path):
        state = "  " if ds.enabled else "off"
        print(f"{state:<3} {ds.frequency}  {ds.source:<5} {ds.id:<34} {ds.dataflow:<36} {ds.title}")
    return 0


def cmd_describe(args, settings: Settings) -> int:
    from .sinks import LocalSink

    client = _clients(settings, LocalSink(settings.output_dir))[args.source]
    structure = client.structure(args.dataflow)
    print(f"{structure.dataflow}: {structure.name}\n")
    for i, dim in enumerate(structure.dimensions, 1):
        codes = structure.labels.get(dim, {})
        print(f"{i:>2}. {dim:<34} {len(codes):>5} codes")
        if args.codes and (args.codes == "all" or dim in args.codes.split(",")):
            for code, label in codes.items():
                print(f"      {code:<28} {label}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="istatsync", description=__doc__)
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="download and publish datasets")
    run.add_argument("only", nargs="*", help="dataset ids (default: all)")
    run.add_argument("--frequency", choices=["A", "Q", "M"])
    run.add_argument("--force", action="store_true", help="republish even if unchanged")
    run.add_argument(
        "--local", action="store_true", help="write to OUTPUT_DIR even if Drive is configured"
    )
    run.set_defaults(func=cmd_run)

    sub.add_parser("list", help="show the catalogue").set_defaults(func=cmd_list)

    desc = sub.add_parser("describe", help="show dimensions and codes of a dataflow")
    desc.add_argument("dataflow")
    desc.add_argument("--codes", metavar="DIM[,DIM]|all", help="also print the codes")
    desc.add_argument("--source", choices=sorted(SOURCES), default="istat")
    desc.set_defaults(func=cmd_describe)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    return args.func(args, Settings.from_env())


if __name__ == "__main__":
    sys.exit(main())
