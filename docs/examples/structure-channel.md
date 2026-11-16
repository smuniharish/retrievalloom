# Structure channel

This example enables the structure channel with an application-owned
`TreeSearchStructureEngine` (in memory). The application provides the document tree, an
admin guide with nested sections, as a `StructureRepresentation` of `StructureNodeInput`
nodes. The structure channel ranks nodes by title and text with TreeSearch's full-text
index.

For each evidence item the example prints:

- the segments with their section paths. A matching node and its parent or children are
  merged into one item (relation `parent_child`);
- for the best-scoring node, its level from the candidate payload
  (`RetrievalExecutionConfig(include_candidates=True)`), and the answers of the structure
  engine's `navigate` method for `PATH`, `PARENT`, `CHILDREN` and `SIBLINGS`. `PATH`
  returns node ids from the root down, in the same order as the section path, which the
  example uses to name the parent.

The application keeps the structure engine to call `navigate` itself: the retrieval
pipeline does not navigate. The engine initialises components passed in `Components` but
does not close them, so the example closes the structure engine itself, while the
retrieval engine and its offloader are still running.

## Requirements

The `structure` extra: `pip install "retrievalloom[structure]"`.

## Run

```bash
python examples/structure_channel.py
```

## Source

<!-- fmt: off -->
```python title="examples/structure_channel.py"
--8<-- "examples/structure_channel.py"
```
<!-- fmt: on -->

## Expected output

```text
query: 'restore the backup'
  #1 score=0.0164 relations=['parent_child']
     Admin guide > Backups > Restore: Stop the service, then copy the backup into the data directory.
     Admin guide > Backups: Back up the data directory nightly.
     top node 'Restore': level=2 parent=['Backups'] children=0 siblings=0
  #2 score=0.0161 relations=['parent_child']
     Admin guide > Installation > Windows: Run the MSI installer and start the service.
     Admin guide > Installation > Linux: Use the apt repository and enable the systemd unit.
     Admin guide > Installation: Install the server package on Linux or Windows.
     top node 'Windows': level=2 parent=['Installation'] children=0 siblings=1
query: 'installation on Linux or Windows'
  #1 score=0.0164 relations=['parent_child']
     Admin guide > Installation: Install the server package on Linux or Windows.
     Admin guide > Installation > Windows: Run the MSI installer and start the service.
     Admin guide > Installation > Linux: Use the apt repository and enable the systemd unit.
     top node 'Installation': level=1 parent=['Admin guide'] children=2 siblings=1
```
