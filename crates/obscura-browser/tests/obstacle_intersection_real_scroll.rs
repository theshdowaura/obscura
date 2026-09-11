#![cfg(feature = "render")]

use obscura_dom::parse_html;
use obscura_js::runtime::ObscuraJsRuntime;
use serde_json::json;

#[tokio::test(flavor = "current_thread")]
async fn obstacle_intersection_loads_five_batches_with_real_exit_and_scroll_entries() {
    let html = include_str!("fixtures/observer-intersection-real-scroll.html");
    let script = html.split_once("<script>").unwrap().1.split_once("</script>").unwrap().0;
    let mut runtime = ObscuraJsRuntime::new();
    runtime.set_dom(parse_html(html));
    runtime.set_viewport(1280.0, 720.0);
    runtime.run_page_init();
    runtime.evaluate(script).unwrap();
    let result = runtime.evaluate_for_cdp(r#"
        new Promise(resolve => {
            const deadline = performance.now() + 3000;
            function poll() {
                if (window.__obstacle || performance.now() >= deadline) {
                    resolve({ result: window.__obstacle,
                        cards: document.querySelectorAll('.card').length,
                        scrolled: window.scrollY > 0,
                        transitions: window.__intersectionTransitions });
                } else setTimeout(poll, 20);
            }
            poll();
        })
    "#, true, true).await.unwrap();
    assert_eq!(result.value.unwrap(), json!({
        "result": "io:50", "cards": 50, "scrolled": true,
        "transitions": [true, false, true, false, true, false, true, false, true],
    }));
}
