use std::cell::UnsafeCell;

pub struct UnsafeWrapper<T> {
    array: UnsafeCell<T>,
}

impl<T> UnsafeWrapper<T> {
    pub fn new(obj: T) -> Self {
        Self {
            array: UnsafeCell::new(obj)
        }
    }

    pub unsafe fn get(&self) -> *mut T {
        self.array.get()
    }

    pub unsafe fn get_mut(&mut self) -> &mut T {
        self.array.get_mut()
    }
}

unsafe impl<T> Sync for UnsafeWrapper<T> {}

unsafe impl<T> Send for UnsafeWrapper<T> {}