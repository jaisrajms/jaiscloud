//go:build gcp_conformance

// Command fidelitygen derives the GCP emulator fidelity matrix from the
// emulator's registries and wire-conformance evidence.
//
// Run from the repo root:
//
//	go run -tags gcp_conformance ./tools/fidelitygen -out docs/fidelity
//
// Each operation/transport cell is classified ga/limited/preview/unsupported
// from the emulator's operation registry, the vendored Discovery schemas, and
// the wire-conformance evidence.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	conf "jaiscloud/tests/gcpconformance"
)

func main() {
	out := flag.String("out", "docs/fidelity", "output directory for the matrix artifacts")
	overridesPath := flag.String("overrides", "docs/fidelity-overrides.yaml", "curated overrides file")
	discoveryDir := flag.String("discovery", "tests/gcpconformance/discovery", "vendored Discovery snapshots dir")
	reportPath := flag.String("report", "tests/gcpconformance/testdata/report/report.json", "conformance report to consume")
	grpcReportPath := flag.String("grpc-report", "tests/gcpconformance/grpc/testdata/report/report.json", "gRPC conformance report to consume")
	transcriptPath := flag.String("transcript", "tests/gcpconformance/testdata/transcripts.json", "committed wire-conformance transcript for response-evidence coverage")
	strict := flag.Bool("strict", false, "also fail if a ga cell carries a non-allowlisted high/medium finding")
	flag.Parse()

	if err := run(*out, *overridesPath, *discoveryDir, *reportPath, *grpcReportPath, *transcriptPath, *strict); err != nil {
		fmt.Fprintln(os.Stderr, "fidelitygen:", err)
		os.Exit(1)
	}
}

func run(out, overridesPath, discoveryDir, reportPath, grpcReportPath, transcriptPath string, strict bool) error {
	ops := conf.Enumerate()

	docs, err := conf.LoadSnapshots(discoveryDir)
	if err != nil {
		return fmt.Errorf("load Discovery snapshots (%s): %w", discoveryDir, err)
	}
	report, err := conf.ReadReport(reportPath)
	if err != nil {
		return fmt.Errorf("read conformance report (%s): %w", reportPath, err)
	}
	grpcReport, err := ReadGRPCReport(grpcReportPath)
	if err != nil {
		return fmt.Errorf("read gRPC conformance report (%s): %w", grpcReportPath, err)
	}
	tr, err := readTranscript(transcriptPath)
	if err != nil {
		return err
	}
	coverage := conf.TranscriptCoverage(ops, docs, tr)
	ov, err := LoadOverrides(overridesPath, ops, conf.EnumerateGRPC())
	if err != nil {
		return err
	}

	grpcFacts := GRPCFacts(conf.EnumerateGRPC(), ov, grpcReport)
	cells, suspicious := classifyAll(ops, docs, report, grpcReport, ov, coverage)

	doc := Render(cells, GRPCOnlyServices(ops, grpcFacts), BuildCoverage(cells, docs))
	if err := ValidateCoverage(cells, doc.Coverage); err != nil {
		return err
	}
	if err := WriteMatrix(out, doc); err != nil {
		return err
	}

	s := doc.Summary
	fmt.Printf("wrote %s: %d cells (ga=%d limited=%d preview=%d unsupported=%d)\n",
		out, s.Total, s.ByState[StateGA], s.ByState[StateLimited],
		s.ByState[StatePreview], s.ByState[StateUnsupported])
	ev := s.Evidence
	fmt.Printf("evidence: rest %d/%d verified, grpc %d/%d verified\n",
		ev.Verified["rest"], ev.Verified["rest"]+ev.Unverified["rest"],
		ev.Verified["grpc"], ev.Verified["grpc"]+ev.Unverified["grpc"])

	derived, unavailable := 0, 0
	for _, e := range doc.Coverage {
		if e.Source == CoverageUnavailable {
			unavailable++
			continue
		}
		derived++
	}
	fmt.Printf("coverage: %d derived record(s), %d unavailable source(s) stated explicitly\n", derived, unavailable)
	for _, svc := range []string{"container", "dataproc"} {
		for _, e := range doc.Coverage {
			if e.Service == svc && e.Source != CoverageUnavailable {
				fmt.Printf("  %s: %s\n", svc, coverageSummary(e))
			}
		}
	}

	if strict && len(suspicious) > 0 {
		return fmt.Errorf("-strict: %d ga cell(s) carry non-allowlisted high/medium findings:\n  %s",
			len(suspicious), strings.Join(suspicious, "\n  "))
	}
	return nil
}

// classifyAll derives one Cell per (operation, transport) fact and returns the
// cells plus the ga cells that carry a non-allowlisted high/medium finding
// (only reachable through an allow_upgrade override; surfaced under -strict).
// It is shared by run() and the generator's unit tests.
func classifyAll(ops []conf.Operation, docs map[string]*conf.DiscoveryDoc, report *conf.Report, grpcReport *grpcReport, ov *Overrides, coverage map[string]int) ([]Cell, []string) {
	grpcFacts := GRPCFacts(conf.EnumerateGRPC(), ov, grpcReport)
	all := append(RestFacts(ops, docs, report, ov, coverage), grpcFacts...)

	cells := make([]Cell, 0, len(all))
	var suspicious []string
	for _, f := range all {
		c := Classify(f)
		cells = append(cells, c)
		if c.State == StateGA {
			if fd, ok := worstUnallowedFinding(f.Findings); ok {
				suspicious = append(suspicious, fmt.Sprintf("%s/%s (%s %s at %s)",
					c.Service, c.Operation, fd.Severity, fd.Kind, fd.Path))
			}
		}
	}
	return cells, suspicious
}

// readTranscript loads the committed wire-conformance transcript used to derive
// REST response-evidence coverage. A missing file yields an empty transcript
// (every mapped op unverified) so a fresh clone without a recording still
// generates the matrix — mirroring ReadGRPCReport's tolerance.
func readTranscript(path string) (conf.Transcript, error) {
	tr, err := conf.ReadTranscript(path)
	if err != nil {
		if os.IsNotExist(err) {
			return conf.Transcript{}, nil
		}
		return conf.Transcript{}, fmt.Errorf("read transcript %s: %w", path, err)
	}
	return tr, nil
}
