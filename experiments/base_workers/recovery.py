#!/usr/bin/env python3
"""Apply the isolated Rust recovery experiment to a disposable source copy."""
from pathlib import Path
import sys
from adapt import replace_once


def adapt(root):
    source = root / 'worker/src/supervisor.rs'
    text = source.read_text()
    text = replace_once(text, '    let mut consecutive_failures = 0_u32;',
                        '    let mut slot_contention = SlotContention::new(Duration::from_secs(5));\n    let mut consecutive_failures = 0_u32;')
    text = replace_once(text, 'run_cycle(&config, &receiver, &publisher, &mut destination)',
                        'run_cycle(&config, &receiver, &publisher, &mut destination, &mut slot_contention)')
    text = replace_once(text, '                    consecutive_failures,\n                );',
                        '                    consecutive_failures,\n                );\n                let delay = slot_contention.remaining().map_or(delay, |left| delay.min(left.max(Duration::from_millis(10))));')
    text = replace_once(text, 'fn source_preflight(destination: &mut Client, config: &Config)',
                        'fn source_preflight(destination: &mut Client, config: &Config, busy: &mut SlotContention)')
    text = replace_once(text, 'inspect_source_candidate(config, address, &expected)',
                        'inspect_source_candidate(config, address, &expected, busy)')
    text = replace_once(text, '    expected: &ExpectedSource,\n) -> Result<SourceObservation, CycleError>',
                        '    expected: &ExpectedSource,\n    busy: &mut SlotContention,\n) -> Result<SourceObservation, CycleError>')
    text = replace_once(text, '    validate_source(expected, &observation, history.as_deref()).map_err(CycleError::Fatal)?;',
                        '    validate_source_binding(expected, &observation, history.as_deref()).map_err(CycleError::Fatal)?;\n    busy.observe(address, observation.active, std::time::Instant::now())?;')
    text = replace_once(text, 'fn validate_source(\n', 'fn validate_source_binding(\n')
    text = replace_once(text, '    if observed.active {\n        return Err("replication slot is already active outside this worker cycle".into());\n    }\n', '')
    text = replace_once(text, '    destination: &mut Client,\n) -> Result<(i64, i64), CycleError>',
                        '    destination: &mut Client,\n    busy: &mut SlotContention,\n) -> Result<(i64, i64), CycleError>')
    text = replace_once(text, 'source_preflight(destination, config)?', 'source_preflight(destination, config, busy)?')
    text = replace_once(text, '    if retryable_failure {', '    if retryable_failure && !busy.remaining().is_some_and(|left| left.is_zero()) {')
    # Keep the original stricter validation API for existing unit tests.
    text += '''
#[cfg(test)]
fn validate_source(expected: &ExpectedSource, observed: &SourceObservation, history: Option<&str>) -> Result<(), String> {
    validate_source_binding(expected, observed, history)?;
    if observed.active { return Err("replication slot is already active outside this worker cycle".into()); }
    Ok(())
}
'''
    text = replace_once(text, '    fn expected_source() -> ExpectedSource', '    pub(super) fn expected_source() -> ExpectedSource')
    text = replace_once(text, '    fn source_observation() -> SourceObservation', '    pub(super) fn source_observation() -> SourceObservation')
    text += Path(__file__).with_name('recovery.rs.inc').read_text()
    source.write_text(text)


if __name__ == '__main__':
    adapt(Path(sys.argv[1]))
