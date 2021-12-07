use std::pin::Pin;
use futures::stream::{self, Stream, StreamExt};

#[derive(Clone)]
struct TestObj {
    pub x: i32,
    pub y: f64
}

#[tokio::main]
async fn main() {
    let outer_obj = TestObj { x: 1, y: 0.5 };
    let test_iter = test_fn(outer_obj);

    let output: Vec<_> = test_iter.collect().await;

    for item in output {
        println!("{}", item);
    }
}

fn test_fn(outer_obj: TestObj) -> impl Stream<Item=bool> {
    stream::iter(vec![0, 1, 3, 2, 4, 6])
        .filter_map(move |val| {
            let cloned = outer_obj.clone();
            async move {
                Some(val == cloned.x)
            }
        })
}