# HTML Review Contract

Use this contract when the requested primary artifact is HTML.

## Outcome

Create one local, self-contained review page that lets the learner understand the day in a glance and complete a 10–15 minute spoken review without opening another application.

## Technical Constraints

- Use semantic HTML, CSS, and Vanilla JS in one file unless the user requests separate files.
- Use no framework, build step, CDN, remote font, remote image, analytics, or network dependency.
- Keep source evidence in a compact local data object; escape user-provided text before inserting it into the DOM.
- Preserve local opening and responsive behavior at desktop and narrow widths.

## Visual Direction

Use a restrained local-study interface: dark, high contrast, information-dense, and calm. Establish clear hierarchy among the daily conclusion, evidence, method gaps, priority questions, and tomorrow route. Avoid marketing layouts, decorative blobs, generic gradients, or visual elements that do not help review.

## Required States

The page must handle:

- verified content;
- `待确认` or `未记录` fields;
- fewer than five priority questions;
- no daily-filter shortlist;
- long source labels and narrow screens.

## Required Content

- date or `待确认`, theme, and evidence status;
- today’s main conclusion and primary risk;
- a compact learning path or timeline when supported;
- evidence-backed error patterns;
- method gaps with trigger, expected first action, and missed action;
- zero to five priority-question cards;
- tomorrow’s ordered actions;
- a 10–15 minute narration script.

Use CSS bars, matrices, or timelines only when the underlying values are verified. Do not turn qualitative impressions into invented counts.

## Interaction

Interaction should reveal or navigate useful study content. Suitable examples include collapsing evidence details, jumping between priority questions, and checking off tomorrow’s actions. The core review must remain usable with JavaScript disabled.

## Verification

Before finalizing:

1. Open or render the file using an available local browser or screenshot route.
2. Check clipping, overflow, contrast, spacing, heading hierarchy, empty states, and mobile layout.
3. Confirm all required evidence and caveats appear in the rendered page, not only in the source data.
4. Revise until the rendered artifact matches the contract.

If rendering is unavailable, validate HTML structure and explain the missing visual check in the completion report.
