use super::Engine;
use crate::traits::EvaluationContext;

/// Trait implemented by data sources that can stream workbook contents into an Engine.
/// This lives in formualizer-eval so IO backends can depend on it without creating cycles.
pub trait EngineLoadStream<R>
where
    R: EvaluationContext,
{
    type Error;
    fn stream_into_engine(&mut self, engine: &mut Engine<R>) -> Result<(), Self::Error>;
}
