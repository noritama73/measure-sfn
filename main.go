package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sfn"
)

var (
	profile = flag.String("profile", "", "AWS profile")
)

func main() {
	flag.Parse()

	if profile == nil || *profile == "" {
		panic("profile is required")
	}

	svc := createSfnSession(*profile)

	machines, err := svc.ListStateMachines(&sfn.ListStateMachinesInput{})
	if err != nil {
		panic(err)
	}

	records := SfnRecords{}

	for _, machine := range machines.StateMachines {
		executions, err := svc.ListExecutions(&sfn.ListExecutionsInput{
			StateMachineArn: machine.StateMachineArn,
		})
		if err != nil {
			panic(err)
		}

		for _, execution := range executions.Executions {
			if execution.StartDate == nil || execution.StopDate == nil {
				continue
			}

			if execution.StartDate.Before(time.Now().AddDate(0, -2, 0)) {
				continue
			}

			duration := execution.StopDate.Sub(*execution.StartDate)

			name := strings.Split(*machine.StateMachineArn, ":")[6]

			records = append(records, SfnRecord{
				Name:      name,
				StartDate: execution.StartDate.Format(time.DateOnly),
				Duration:  duration,
				Status:    *execution.Status,
			})
		}
	}

	if err := createCsvFile(records); err != nil {
		panic(err)
	}

	if err := records.aggregate(); err != nil {
		panic(err)
	}
}

type SfnRecord struct {
	Name      string        `csv:"Name"`
	StartDate string        `csv:"StartDate"`
	Duration  time.Duration `csv:"Duration"`
	Status    string        `csv:"Status"`
}

func (r SfnRecord) StringDurationSecond() string {
	return fmt.Sprintf("%.2f", r.Duration.Seconds())
}

type SfnRecords []SfnRecord

func (r SfnRecords) MaxDuration() time.Duration {
	max := r[0].Duration
	for _, record := range r {
		if record.Duration > max {
			max = record.Duration
		}
	}
	return max
}

func (r SfnRecords) MinDuration() time.Duration {
	min := r[0].Duration
	for _, record := range r {
		if record.Duration < min {
			min = record.Duration
		}
	}
	return min
}

func (r SfnRecords) AvgDuration() time.Duration {
	total := time.Duration(0)
	for _, record := range r {
		total += record.Duration
	}
	return total / time.Duration(len(r))
}

func (r SfnRecords) Len() int {
	return len(r)
}

func createCsvFile(records SfnRecords) error {
	w, err := os.Create("sfn.csv")
	if err != nil {
		return err
	}
	defer w.Close()
	writer := csv.NewWriter(w)

	if err := writer.Write([]string{"Name", "StartDate", "Duration", "Status"}); err != nil {
		return err
	}

	for _, record := range records {
		if err := writer.Write([]string{record.Name, record.StartDate, record.StringDurationSecond(), record.Status}); err != nil {
			return err
		}
	}

	writer.Flush()
	return writer.Error()
}

func createSfnSession(profile string) *sfn.SFN {
	opt := session.Options{
		Config:                  *aws.NewConfig(),
		Profile:                 profile,
		AssumeRoleTokenProvider: stscreds.StdinTokenProvider,
		AssumeRoleDuration:      3600 * time.Second,
		SharedConfigState:       session.SharedConfigEnable,
	}
	sess := session.Must(session.NewSessionWithOptions(opt))

	return sfn.New(sess)
}

type AggregatedRecordMap map[string]SfnRecords

func (r *SfnRecords) aggregate() error {
	aggregated := make(AggregatedRecordMap)
	for _, record := range *r {
		aggregated[record.Name] = append(aggregated[record.Name], record)
	}

	if err := createAggregateCsvFile(aggregated); err != nil {
		return err
	}

	return nil
}

func createAggregateCsvFile(records AggregatedRecordMap) error {
	w, err := os.Create("aggregate.csv")
	if err != nil {
		return err
	}
	defer w.Close()
	writer := csv.NewWriter(w)

	if err := writer.Write([]string{"Name", "Max", "Min", "Avg", "Len"}); err != nil {
		return err
	}

	for name, records := range records {
		if err := writer.Write([]string{
			name,
			durationToSeconfString(records.MaxDuration()),
			durationToSeconfString(records.MinDuration()),
			durationToSeconfString(records.AvgDuration()),
			fmt.Sprintf("%d", records.Len()),
		}); err != nil {
			return err
		}
	}

	writer.Flush()
	return writer.Error()
}

func durationToSeconfString(d time.Duration) string {
	return fmt.Sprintf("%.2f", d.Seconds())
}
