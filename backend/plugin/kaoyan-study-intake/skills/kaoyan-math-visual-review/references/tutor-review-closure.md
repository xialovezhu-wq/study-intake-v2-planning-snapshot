# Tutor Review Closure

Use this reference only when the user explicitly asks to sync the review into Tutor or refresh the StudyVault.

## Goal

Transfer reusable, high-level learning signals without copying protected problem content or treating Tutor output as formal mastery evidence.

## Allowed Targets

- `错题知识网络/wiki/study_vaults/数学LLMWiki/tutor_setup_sources/`
- `错题知识网络/wiki/study_vaults/数学LLMWiki/StudyVault/`

Do not write to formal cards, generated data, rollback files, or the repository root.

## Allowed Content

- date or `待确认`;
- involved formal IDs;
- topics and weak concepts;
- method-gap types, triggers, and first actions;
- repeated error patterns;
- explicit next drill targets.

Do not include full problem statements, long solutions, screenshots, answer keys, invented scores, or raw Chronicle history.

## Route

If the StudyVault is absent and the user asked to create or refresh it, follow `tutor-setup` on the safe source pack. If it exists, update only the relevant safe source and verify the affected dashboard path. Do not start a quiz unless the user asks to be tested now.

Report the changed source, StudyVault action, Tutor target, and confirmation that no formal card or rollback score was changed.
