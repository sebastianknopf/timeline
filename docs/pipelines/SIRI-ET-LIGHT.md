# Pipeline Definition: SIRI ET (Light)

## Title

SIRI Estimated Timetable (Light) Pipeline

## Name

`siri-et-light`

This value is used in configuration as `pipeline.name`.

## Description

SIRI ET Light is the realtime-pipeline definition for SIRI-ET data.

This pipeline is expected to run at high frequency, typically at least once per minute.

The pipeline fetches source XML data, transforms it into the normalized Timeline model, and hands the normalized output to the central load service.

Reference: SIRI-CEN Specs on Github: [https://github.com/TransmodelEcosystem/SIRI](https://github.com/TransmodelEcosystem/SIRI).

**Note: Please note that SIRI-ET Light is no official standard and supposed to be a project / producer specific input most times! Normally, SIRI-ET uses a abo / subscribe protocol or a request / response protocol using POST requests. The SIRI-ET Light implementation here uses just GET requests against a given URL with the configured authentication.**

## Shared Configuration Parameters

This pipeline uses these shared configuration keys:

- `id`
- `name` (must be `siri-et-light`)
- `type` (must be `realtime` for this pipeline)
- `cron`
- `policy`
- `endpoint`
- `priority`
- `authentication` (optional)
- `filter` (optional)
     - `routes`
     - `operators`

## Optional Pipeline Parameters

The pipeline allows the definition of pipeline specific configuration keys:

| Name | Type | Description |
| ---- | ---- | ---- |
| `dpl_appendix_pattern` | String (optional) | Pattern for identifying appendix to the real IDs which may be extended by the data platforms for collecting the data. Default: `None` |
| `operation_day_end` | String (optional) | Time (hh:mm in the following day) when the operating day ends. Default: `03:00` |

Many data integration platforms collecting and forwarding the SIRI-ET data add an appendix to the real IDs originally delivered by the data suppliers. This appendix is typically separated by a certain pattern like '`[OriginalID]#!ADD!#[Appendix]`' where `#!ADD!#` is the pattern. To tear down to the originally delivered IDs, the pipeline specific parameter `dpl_appendix_pattern` can be set to this pattern. If set, all IDs are separated by this pattern and only the first remaining part is taken into further processing.

## Filtering
During the execution, the pipeline respects the optional `filter` parameters. Filter types (include/exclude) are respeced. If at least one filter is set, only data matching those filters are imported. Routes are filtered against the `LineRef` based on `filter.routes[...].match`. Operators are filtered against the `OperatorRef` based on `filters.operators[...].match` if present. If the `OperatorRef` is not set or empty, the trip is filtered out. If no `filter` is set at all, the whole feed will be imported.

**Note: Please be aware that the `OperatorRef` element in a EstimatedVehicleJourney is optional and could be empty! This means that EstimatedVehicleJourneys without having `OperatorRef` set are discarded entirely if a filter on operator is set!**

## Assumptions

| ID | Assumption | Reason / Impact |
| --- | --- | --- |
| A1 | The endpoint returns a valid `ServiceDelivery` containing `EstimatedVehicleJourney` objects. | Each `EstimatedVehicleJourney` object is treaded as one trip in Timeline. |
| A2 | `operation_day_date` is extracted from `EstimatedVehicleJourney.FramedVehicleJourneyRef.DataFrameRef` if available. If not available, it must be derived out of the data of the trip. | Supports late/overlapping realtime updates (for example updates still referencing yesterday service date). |
| A3 | `instance_id` is injected from runtime pipeline instance context. | GTFS Realtime has no native tenant key. |
| A4 | `EstimatedVehicleJourney` resolves to a single trip instance. | Prevents ambiguous writes across multiple trip instances. |
| A5 | `trip_id` is extracted from `EstimatedVehicleJourney.DatedVehicleJourneyRef` or `EstimatedVehicleJourney.VehicleJourneyRef` as fallback. | Extracts the ID of the nominal trip and the ID of the realtime trip as fallback for matching against the nominal timetable. |
| A6 | `concessionaire_id` is extracted from `EstimatedVehicleJourney.OperatorRef` and `concessionaire_name` remains null in this pipeline. The naming is misleading, but in fact, the `OperatorRef` in SIRI represents the concessionaire typically. | Keeps reference to a certain concessionaire. Not imported into the tracked database, but importand for the quality issues. |
| A7 | `operator_id` and `operator_name` remain null in this pipeline. | Keeps semantics consistent with current database model decisions. |
| A8 | Each `EstimatedVehicleJourney.EstimatedCalls.EstimatedCall` represents one stop time in the trip object. | Proper extraction of all stop times. |
| A9 | The field `EstimatedVehicleJourney.LineRef` represents the line ID. Must be mapped with the mapping service. | Derive line ID for matching the trip against the nominal timetable. |
| A10 | A `EstimatedCall` represents a stop time with realtime data available, a `RecordedCall` represents data which are recorded. Both are transformed into stop times. | Keep track of actual monitoring data and recorded monitoring data. |
| A11 | `EstimatedCall.StopPointRef` maps to the stop ID. Must be mapped with the mapping service. | Reference stop IDs in the nominal timetable. |
| A12 | `EstimatedCall.AimedDepartureTime` is the nominal departure time (or arrival time for `AimedArrivalTime`). | Determine nominal times for later matching based on the stop sequence. |
| A13 | `EstimatedCall.ExpectedDepartureTime` is the actual departure time (or arrival time for `ExpectedArrivalTime`) for a particular stop. | Determine the actual schedule data for the particular stop. |
| A14 | `EstimatedCall.Monitored` indicates whether a trip has proper realtime data or not. If set to `false`, the trip is discarded. | Identify trips which don't have realtime data at all. |
| A15 | `EstimatedVehicleJourney.IsCompleteStopSequence` states that the `EstimatedCalls` structure is a complete set of stop times seen from the data producers perspective. | Indicate this field for `_t_is_complete_stop_sequence` for quality monitoring. |
| A16 | `EstimatedVehicleJourney.OriginRef` and `EstimatedVehicleJourney.DestinationRef` are only used as fallback when the first and last stop cannot be derived because `EstimatedVehicleJourney.IsCompleteStopSequence` is false or not set. | Determine first and last stop for `_t_scheduled_start_stop_id` and `_t_scheduled_end_stop_id` for later matching against the nominal timetable. |
| A17 | Added trips and stop times are completely discarded. | Both types are not supported currently. Don't spam the database ... |

## Transformations

| Step | Input | Transformation | Output |
| T1 | SIRI-ET stream from `endpoint` | Stream the results and process it sequencially. | Stream of `EstimatedVehicleJourney` objects. |
| T2 | `EstimatedVehicleJourney.FramedVehicleJourneyRef.DataFrameRef` is used for the operating day. If not available, the `EstimatedVehicleJourney.EstimatedCalls[0].EstimatedCall/RecordedCall` (aimed departure time (if not available: aimed arrival time) is extracted for the operation day date. Then, if this timestamp is checked against the pipeline property `operation_day_end` whether the timestamp is before or after this timestamp; if it is before or equal this timestamp, the **date before is used as operating day**, otherwise the current date is used as operating day. | Valid operation day date. |
| T3 | `EstimatedVehicleJourney.VehicleJourneyRef` is used for the trip ID. Alternatively `EstimatedVehicleJourney.DatedVehicleJourneyRef` is used. | A valid trip ID. |
| T4 | `EstimatedVehicleJourney.EstimatedCalls` are transformed into stop times with stop ID, nominal arrival and departure times and actual departure and arrival times | A list of stop times per trip instance. |
| T5 | `EstimatedVehicleJourney.EstimatedCalls[0]` and `EstimatedVehicleJourney.EstimatedCalls[-1]`; `EstimatedVehicleJourney.OriginRef` and `EstimatedVehicleJourney.DestinationRef` as fallback and `EstimatedVehicleJourney.IsCompleteStopSequence` are used to fill the `_t_...` identification fields. | Fields for matching against the nominal timetable. |
| T6 | `EstimatedVehicleJourney.Cancellation` is set to true, the trip is considered as schedule relationship 'CANCELED'. If `EstimatedVehicleJourney.Monitored` is set to true without cancellation, the trip is considered as schedule relationship 'SCHEDULED'. | Valid schedule relationship for the trip object. |
| T7 | `EstimatedCall/RecordedCall.Cancellation` is treaded as schedule relationship 'CANCELED' for the particular stop time if set to true. Otherwise the schedule relationship of the stop times is set to 'SCHEDULED' or 'UNKNOWN' depending on the schedule relationship of the trip. | Valid schedule relation for the stop time object. |
| T8 | `EstimatedCall/RecordedCall.ExtraCall` set to true **is ignored** as added stop times are currently not supported. `EstimatedVehicleJourney.ExtraTrip` set to true **is also ignored** as added trips are currently not supported. | Filtering out added trips and stop times. |

## Mappings

| Timeline Entity | Timeline Field | Source (SIRI-ET) | Mapping Rule |
| --- | --- | --- | --- |
| `dim_trips` | `instance_id` | runtime instance | Injected from the pipeline context. |
| `dim_trips` | `operation_day_date` | formerly determined operation day date | Extracted by the above defined transformation rules. |
| `dim_trips` | `trip_id` | formerly determined trip ID | Extracted by the above defined transformation rules. |
| `dim_trips` | `act_start_time` | `EstimatedVehicleJourney.EstimatedCalls[0]/RecordedCalls[0].ExpectedDepartureTime/ExpectedArrivalTime` (in that sequence for fallback) or `EstimatedVehicleJourney.EstimatedCalls[0]/RecordedCalls[0].AimedDepartureTime/AimedArrivalTime` (in that sequence for fallback) as generall fallback **if not estimated data available** for the particular stop time. | Datetimes all in ISO8601 format, need to be parsed for datetime objects. |
| `dim_trips` | `act_end_time` | `EstimatedVehicleJourney.EstimatedCalls[-1]/RecordedCalls[-1].ExpectedArrivalTime/ExpectedDepartureTime` (in that sequence for fallback) or `EstimatedVehicleJourney.EstimatedCalls[-1]/RecordedCalls[-1].AimedArrivalTime/AimedDepartureTime` (in that sequence for fallback) as generall fallback **if not estimated data available** for the particular stop time. | Datetimes all in ISO8601 format, need to be parsed for datetime objects. |
| `dim_trips` | `schedule_relationship` | formerly depermined schedule relationshop on trip level | Extracted by the above defined transformation rules. |
| `fact_stop_times` | `instance_id` | runtime instance | Injected from the pipeline context. |
| `fact_stop_times` | `operation_day_date` | formerly determined operation day date | Extracted by the above defined transformation rules. |
| `fact_stop_times` | `trip_id` | formerly determined trip ID | Extracted by the above defined transformation rules. |
| `fact_stop_times` | `stop_id` | `...EstimatedCalls/RecordedCalls.EstimatedCall/RecordedCall.StopRef` | Used after mapping for the stop ID. |
| `fact_stop_times` | `stop_sequence` | **not available in the data themselves** | Impliticly generated by an increasing number over all contained stop times |
| `fact_stop_times` | `distance_from_start` | nominal baseline | Taken from the matching nominal stop-time row (keyed by `stop_sequence`) in the central load service. Set to `0.0` as an initial placeholder; replaced with the correct nominal value before every DB write when nominal data is loaded. Never computed from `stop_sequence` or event timestamps. |
| `fact_stop_times` | `act_arrival_time` | `...EstimatedCalls/RecordedCalls.EstimatedCall/RecordedCall.EstimatedArrivalTime` | Prefer the estimated fields if available; fall back to aimed fields, prefer the arrival time fields if available; fall back to the departure time fields. |
| `fact_stop_times` | `act_departure_time` | `...EstimatedCalls/RecordedCalls.EstimatedCall/RecordedCall.EstimatedDepartureTime` | Prefer the estimated fields if available; fall back to aimed fields, prefer the departure time fields if available; fall back to the arrival time fields. |
| `fact_stop_times` | `schedule_relationship` | formerly determined schedule relationship on stop time level | Latest-wins update per stop row; determined schedule relationship by the transformation rules above. |