use std::error::Error;

type RollbackFn = Box<dyn FnMut() -> Result<(), Box<dyn Error + Send + Sync>> + Send + 'static>;

pub struct Rollback {
    actions: Vec<RollbackFn>,
    committed: bool,
}

impl Rollback {
    pub fn new() -> Self {
        Rollback {
            actions: Vec::new(),
            committed: false,
        }
    }

    pub fn push<F>(&mut self, action: F)
    where
        F: FnMut() -> Result<(), Box<dyn Error + Send + Sync>> + Send + 'static,
    {
        self.actions.push(Box::new(action));
    }

    pub fn commit(mut self) {
        self.committed = true;
        self.actions.clear();
    }

    fn rollback(&mut self) {
        for action in self.actions.iter_mut().rev() {
            let _ = action();
        }
        self.actions.clear();
    }
}

impl Drop for Rollback {
    fn drop(&mut self) {
        if !self.committed {
            self.rollback();
        }
    }
}
