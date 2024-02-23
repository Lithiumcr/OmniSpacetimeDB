use super::*;

pub struct ObserverManager {
    pub(crate) observers: Vec<(ObserverId, Arc<dyn DurabilityObserver>)>,
    counter: u64,
}

impl ObserverManager {
    pub fn new() -> Self {
        Self {
            observers: Vec::new(),
            counter: 0,
        }
    }

    pub fn register(&mut self, observer: Arc<dyn DurabilityObserver>) -> ObserverId {
        // I need the ObserverId to be stable so that I can use it to unregister the observer later.
        // I can't use the length of the observers vector as the ObserverId because the length of the vector can change.
        // I can't use the index of the observer in the vector as the ObserverId because the index of the observer in the vector can change.

        // I can use a counter to generate the ObserverId.
        let observer_id = ObserverId(self.counter);
        self.counter += 1;

        // Associate the observer with its id in the observers vector.
        self.observers.push((observer_id, observer));

        observer_id
    }

    pub fn unregister(&mut self, observer_id: ObserverId) {
        // Find the observer in the observers vector.
        let index = self
            .observers
            .iter()
            .position(|(id, _)| id.0 == observer_id.0)
            .unwrap();

        // Remove the observer from the observers vector.
        self.observers.remove(index);
    }

    pub fn len(&self) -> usize {
        self.observers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.observers.is_empty()
    }
}

impl Default for ObserverManager {
    fn default() -> Self {
        Self::new()
    }
}
