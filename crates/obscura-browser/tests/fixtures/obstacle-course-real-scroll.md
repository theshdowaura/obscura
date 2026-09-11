# Obstacle-course real-scroll fixture

Upstream benchmark base: `6ebac8293d7477f59e837768bfd4e74173f04f1c`.
Runner: `obstacle-course/run.py`; original case: `obstacle-course/fixtures/observer-intersection.html`.

The original 33-case run passed 32 cases. The intersection case expected `io:50` from a single observation without a scroll or a new observation. The renderer correctly loaded 10 items and retained the intersecting state; its existing native test `intersection_observer_does_not_refire_while_target_stays_intersecting` explicitly requires this behavior.

The patch keeps all 33 cases and the `io:50` assertion. Each batch now pushes the sentinel outside the viewport; two animation frames allow the exit notification before a simulated real scroll brings it back. It never synthesizes observer events or changes engine behavior. Native coverage asserts 50 cards, positive scroll offset, and alternating entry/exit notifications.

Validation on the render release binary SHA-256 `98686b1085a2277e9ff9fa10fd657b6e67e8bfedc11fb08a505904e9a56a4e68`: original fixture set **32/33**; corrected fixture set **33/33**. A separate native CLI check observed 50 cards, scroll offset 3289, and `[true,false,true,false,true,false,true,false,true]`. The original result is retained as the baseline; it is not reported as passing.

Apply the adjacent patch from the benchmark root and run:

```sh
git apply /absolute/path/to/obscura-benchmark-intersection-real-scroll.patch
OBSCURA_BIN=/absolute/path/to/obscura OBSCURA_ALLOW_PRIVATE_NETWORK=1 python3 obstacle-course/run.py --runs 1 --warmup 0
```

Private networking is enabled only for the local benchmark HTTP fixtures. Production requestGuard rejects that opt-in.

SHA-256:

- Original fixture: `b6928f9c0d8c19dedfb0b63497be212731e3f18ce75e7c6524b8239abc27d97e`
- Corrected fixture: `a6ea2ce89fb65f4681b8c8dfb05cad1759911ed19c97e42bc24b4cea7c8a4958`
- Review patch: `cf6dba6da8872abfac3108c14a2ac305cb5acab105b12952447b3727f1f2a29f`
