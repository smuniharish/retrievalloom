# Policy manual

Policies, manuals and contracts are long and hierarchical. An answer lives in one section,
and readers trust it more when they see the section path, for example
"Travel and Expense Policy > Expenses > Submitting receipts". The example searches a travel
and expense policy alongside two short documents: an intranet FAQ that copied a policy
paragraph, and a finance news item.

## How it works

- **Structure.** The application passes the manual's outline as a `StructureRepresentation`
  of `StructureNodeInput` sections (`data/travel_policy.json`). The structure channel
  (TreeSearch) ranks sections by title and text and reports the section path.
- **Index channel.** The index channel searches the same corpus lexically, and the two
  channels are fused.
- **`NearDuplicateDeduplicator`** (a custom `DeduplicatorABC`) merges candidates whose text
  is almost identical. The FAQ copy and the policy section become one evidence item that
  cites both sources.
- **`DocumentDiversityConsolidator`** (a custom `EvidenceConsolidatorABC`) caps how many
  items one document contributes (2 here), so the manual cannot crowd out other sources.
- **`StemmingTokenizer`** makes `receipts`/`receipt` and `submitting`/`submit` match.

A custom `StructureEngineABC` is not included. The default TreeSearch engine already
handles any outline the application supplies. A replacement is worth writing only when
the structure lives elsewhere, for example in a document store with its own section
search.

## Requirements

The `structure` extra: `pip install "retrievalloom[structure]"`.

## Run

```bash
python examples/policy_manual.py
```

## Source

<!-- fmt: off -->
```python title="examples/policy_manual.py"
--8<-- "examples/policy_manual.py"
```
<!-- fmt: on -->

## Expected output

```text
Q: When do I have to submit my receipt?
  #1 ['index', 'structure'] expense-faq: (whole text) | travel-policy: Travel and Expense Policy > Expenses > Submitting receipts
      Expense FAQ: Upload receipts to the expense tool within 14 days of returning. Claims submitted later than 60 d
  #2 ['structure'] travel-policy: Travel and Expense Policy > Expenses > Lost receipts
      A lost receipt can be replaced by a signed missing-receipt declaration for amounts up to 50 euros.
  #3 ['index'] finance-news: (whole text)
      Reminder from finance: the expense tool now accepts photos of receipts from your phone.
  items from the manual: 1 (cap 2)
Q: hotel price limit per night in Europe
  #1 ['structure'] travel-policy: Travel and Expense Policy > Booking travel > Hotels
      Hotel rates are capped at 180 euros per night in Europe and 250 dollars per night in North America, breakfast
  ...
Q: Can I fly business class?
  #1 ['structure'] travel-policy: Travel and Expense Policy > Booking travel > Flights
      Economy class is standard. Business class is allowed for flights longer than six hours with approval from your
  ...
Q: security briefing for risky trips
  #1 ['structure'] travel-policy: Travel and Expense Policy > Safety > High-risk destinations
      Travel to high-risk destinations requires a security briefing and approval from the chief security officer.
  ...
```

In the first answer the FAQ and the policy section are merged into one item with two
sources, found by both channels. No result ever holds more than two items from the manual.
