# Creating a PAQS Package

The PAQS package is generated automatically using the helper function defined in `query/script/base/setup.R`:

```r
create_paqs_package()
```

- Creates a **containerized** package (`*_docker.zip`).
- Creates a **native R** package (`*_nativeR.zip`).
- Generates a workplan HTML (`*_workplan.html`).

Running `create_paqs_package()` will produce both zip files under the `package/` directory along with the workplan, ready for distribution.
