use std::{sync::Arc, task::{RawWaker, RawWakerVTable, Wake, Waker}};

pub struct SocketWaker;

impl Wake for SocketWaker {
    fn wake(self: Arc<Self>) {}
}

impl SocketWaker {

    pub fn new() -> Waker {
        unsafe {
            static VTABLE: RawWakerVTable = RawWakerVTable::new(
                |data| RawWaker::new(data, &VTABLE),
                |data| unsafe {
                    let waker = Arc::from_raw(data as *const SocketWaker);
                    waker.clone().wake();
                    std::mem::forget(waker); // Prevent double free
                },
                |data| unsafe {
                    let waker = Arc::from_raw(data as *const SocketWaker);
                    waker.clone().wake();
                    std::mem::forget(waker);
                },
                |data| unsafe {
                    let _ = Arc::from_raw(data as *const SocketWaker);
                },
            );
    
            let raw_waker = RawWaker::new(
                Arc::into_raw(Arc::new(SocketWaker)) as *const (),
                &VTABLE,
            );
            Waker::from_raw(raw_waker)
        }
    }
}
