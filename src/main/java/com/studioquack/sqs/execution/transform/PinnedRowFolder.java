package com.studioquack.sqs.execution.transform;

import com.studioquack.common.dto.flow.InOutValueDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Connection lines decide the message count; attribute mappings never do.
 *
 * <p>A consuming port fed by N connection lines carries exactly N messages — one per line
 * (A→C and B→C into {@code C.input} is a list of two). Everything mapped on ONE DuckTape screen —
 * the line's own wire and every Add-Node (pinned) tile added to that screen's scope — belongs to
 * that line's single message. So each pinned row is folded into the message of the line it was
 * pinned on (its <b>carrier</b>, the line's source node) and removed as a row of its own.
 *
 * <p>Runs right after the inbound JOLT pre-bake, when every row already carries the consuming
 * port's shape. Anchor for a pinned row, on the same port:
 * <ol>
 *   <li>the wire row produced by the carrier ({@code nodeId == carrier}, not a gateway hop);</li>
 *   <li>else — the carrier is a gateway — the look-through wire row ({@code gateway == true});</li>
 *   <li>else the port's sole wire row, but only when identity is unknown (blank carrier or a wire
 *       row without {@code nodeId}) — a sole wire from a DIFFERENT producer is another line;</li>
 *   <li>else the pinned row itself becomes the line's message (the line's own wire delivered
 *       nothing, e.g. its source was skipped); later tiles on the same line fold into it.</li>
 * </ol>
 * Merge: object + object key-merges (the wire's keys win, the pinned row fills the rest);
 * list + list is the wire's items followed by the pinned items; a pinned value whose shape cannot
 * merge into the wire's (scalar clash, object vs list) is dropped with a WARN naming the port —
 * never sent as a second message. Rows that are not pinned are never touched, so the N real
 * lines still deliver N messages.
 *
 * <p>Shared by {@code DefaultTransformPipeline} (sandbox / stateful nodes) and
 * {@code DoclingDelegate} (its own inbound leg) so both consumers honour the same contract.
 */
@Slf4j
public final class PinnedRowFolder {

    private PinnedRowFolder() {
    }

    /** Outcome: the rows to keep, how many pinned rows folded in, how many pinned values were dropped. */
    public record Result(List<InOutValueDto> rows, int folded, int dropped) {
        public boolean changed() {
            return folded > 0;
        }
    }

    /**
     * @param rows     the consuming node's {@code inputList} after the JOLT pre-bake
     * @param carriers {@code bindingKey → carrier} for the pinned rows
     *                 ({@link ForeignEdgeInputResolver#pinnedCarriers})
     * @param nodeId   consuming node, for the log line only
     */
    public static Result fold(List<InOutValueDto> rows, Map<String, String> carriers, String nodeId) {
        if (rows == null || rows.isEmpty() || carriers == null || carriers.isEmpty()) {
            return new Result(rows, 0, 0);
        }
        List<InOutValueDto> wires = new ArrayList<>();
        for (InOutValueDto row : rows) {
            if (row != null && !carriers.containsKey(ForeignEdgeInputResolver.bindingKey(row))) {
                wires.add(row);
            }
        }
        // (port|carrier) → the pinned row that became the line's message when its wire was absent.
        Map<String, InOutValueDto> lineAnchors = new LinkedHashMap<>();
        List<InOutValueDto> kept = new ArrayList<>(rows.size());
        int folded = 0;
        int dropped = 0;
        for (InOutValueDto row : rows) {
            if (row == null) {
                continue;
            }
            String key = ForeignEdgeInputResolver.bindingKey(row);
            String port = portKey(row);
            if (port == null || !carriers.containsKey(key)) {
                kept.add(row);
                continue;
            }
            String carrier = carriers.get(key);
            InOutValueDto anchor = wireOf(wires, port, carrier);
            if (anchor == null) {
                anchor = lineAnchors.get(port + '|' + carrier);
            }
            if (anchor == null) {
                lineAnchors.put(port + '|' + carrier, row);
                kept.add(row);
                continue;
            }
            Merge merge = merge(anchor.getValue(), row.getValue());
            anchor.setValue(merge.value);
            folded++;
            if (merge.dropped) {
                dropped++;
                log.warn("pinned-fold: node={} port={} carrier={} pinned value dropped — its shape ({}) cannot "
                                + "merge into the line's ({}); map both sources onto object fields of the port",
                        nodeId, port, carrier, shape(row.getValue()), shape(anchor.getValue()));
            }
        }
        if (folded > 0) {
            log.info("pinned-fold: node={} folded={} dropped={} rows={}", nodeId, folded, dropped, kept.size());
        }
        return new Result(kept, folded, dropped);
    }

    /** The wire row of the carrier's connection line on {@code port}, per the anchor order above. */
    private static InOutValueDto wireOf(List<InOutValueDto> wires, String port, String carrier) {
        InOutValueDto lookThrough = null;
        InOutValueDto sole = null;
        int onPort = 0;
        for (InOutValueDto wire : wires) {
            if (!port.equals(portKey(wire))) {
                continue;
            }
            onPort++;
            sole = wire;
            if (carrier != null && carrier.equals(wire.getNodeId()) && !wire.isGateway()) {
                return wire;
            }
            if (wire.isGateway() && lookThrough == null) {
                lookThrough = wire;
            }
        }
        if (lookThrough != null) {
            return lookThrough;
        }
        // A sole wire is the line only when nobody can say otherwise. A wire that names a different
        // producer is a different connection line; the carrier's line then delivered nothing.
        boolean identityUnknown = StringUtils.isBlank(carrier) || (sole != null && StringUtils.isBlank(sole.getNodeId()));
        return onPort == 1 && identityUnknown ? sole : null;
    }

    private record Merge(Object value, boolean dropped) {
    }

    @SuppressWarnings("unchecked")
    private static Merge merge(Object anchor, Object pinned) {
        if (anchor == null) {
            return new Merge(pinned, false);
        }
        if (pinned == null) {
            return new Merge(anchor, false);
        }
        if (anchor instanceof Map<?, ?> base && pinned instanceof Map<?, ?> add) {
            return new Merge(mergeMaps(base, add), false);
        }
        if (anchor instanceof List<?> base && pinned instanceof List<?> add) {
            List<Object> out = new ArrayList<>(base.size() + add.size());
            out.addAll((List<Object>) base);
            out.addAll((List<Object>) add);
            return new Merge(out, false);
        }
        return new Merge(anchor, true);
    }

    /** Anchor keys win; the pinned map contributes the keys the anchor did not set; nested maps recurse. */
    private static Map<String, Object> mergeMaps(Map<?, ?> base, Map<?, ?> add) {
        Map<String, Object> out = new LinkedHashMap<>();
        base.forEach((k, v) -> out.put(String.valueOf(k), v));
        for (Map.Entry<?, ?> e : add.entrySet()) {
            String key = String.valueOf(e.getKey());
            if (!out.containsKey(key)) {
                out.put(key, e.getValue());
            } else if (out.get(key) instanceof Map<?, ?> em && e.getValue() instanceof Map<?, ?> am) {
                out.put(key, mergeMaps(em, am));
            }
        }
        return out;
    }

    private static String portKey(InOutValueDto row) {
        String key = StringUtils.firstNonEmpty(row.getTargetPortValue(), row.getPortValue());
        return StringUtils.isEmpty(key) ? null : key;
    }

    /** Shape word for the WARN — never the value. */
    private static String shape(Object v) {
        if (v == null) {
            return "null";
        }
        if (v instanceof Map<?, ?>) {
            return "object";
        }
        if (v instanceof List<?>) {
            return "list";
        }
        return v.getClass().getSimpleName().toLowerCase(java.util.Locale.ROOT);
    }
}
