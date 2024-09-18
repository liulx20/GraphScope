/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir.planner.rules;

import com.alibaba.graphscope.common.ir.rel.GraphShuttle;
import com.alibaba.graphscope.common.ir.rel.graph.*;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.type.AliasNameWithId;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.rex.RexVariableAliasCollector;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class FlatJoinRule extends GraphShuttle {
    private static final Logger logger = LoggerFactory.getLogger(FlatJoinRule.class);

    protected abstract boolean matches(LogicalJoin join);

    protected abstract RelNode perform(LogicalJoin join);

    @Override
    public RelNode visit(LogicalJoin join) {
        join = (LogicalJoin) visitChildren(join);
        return matches(join) ? perform(join) : join;
    }

    static @Nullable RexGraphVariable joinByOneColumn(RexNode condition) {
        List<RexGraphVariable> vars =
                condition.accept(new RexVariableAliasCollector<>(true, var -> var));
        return (vars.size() == 2 && vars.get(0).getAliasId() == vars.get(1).getAliasId())
                ? vars.get(0)
                : null;
    }

    static List<RexGraphVariable> joinByTwoColumns(RexNode condition) {
        List<RexNode> conditions = RelOptUtil.conjunctions(condition);
        if (conditions.size() != 2) return ImmutableList.of();
        RexGraphVariable var1 = joinByOneColumn(conditions.get(0));
        RexGraphVariable var2 = joinByOneColumn(conditions.get(1));
        return (var1 != null && var2 != null && var1.getAliasId() != var2.getAliasId())
                ? ImmutableList.of(var1, var2)
                : ImmutableList.of();
    }

    static boolean hasNodeFilter(RelNode top) {
        if (top instanceof GraphLogicalSource) {
            GraphLogicalSource source = (GraphLogicalSource) top;
            if (source.getUniqueKeyFilters() != null || ObjectUtils.isNotEmpty(source.getFilters()))
                return true;
        }
        if (top instanceof GraphLogicalGetV) {
            GraphLogicalGetV getV = (GraphLogicalGetV) top;
            if (ObjectUtils.isNotEmpty(getV.getFilters())) return true;
        }
        return top.getInputs().stream().anyMatch(k -> hasNodeFilter(k));
    }

    static boolean hasPxdWithUntil(RelNode top) {
        if ((top instanceof GraphLogicalPathExpand)
                && ((GraphLogicalPathExpand) top).getUntilCondition() != null) {
            return true;
        }
        return top.getInputs().stream().anyMatch(k -> hasPxdWithUntil(k));
    }

    static GraphLogicalSource getSource(RelNode top) {
        if (top instanceof GraphLogicalSource) {
            return (GraphLogicalSource) top;
        }
        if (top.getInputs().isEmpty()) return null;
        return getSource(top.getInput(0));
    }

    static RelNode setStartAlias(RelNode top, AliasNameWithId startAlias) {
        if (top instanceof GraphLogicalExpand) {
            GraphLogicalExpand expand = (GraphLogicalExpand) top;
            return GraphLogicalExpand.create(
                    (GraphOptCluster) expand.getCluster(),
                    expand.getHints(),
                    expand.getInput(0),
                    expand.getOpt(),
                    expand.getTableConfig(),
                    expand.getAliasName(),
                    startAlias,
                    expand.isOptional(),
                    expand.getFilters(),
                    (GraphSchemaType) expand.getRowType().getFieldList().get(0).getType());
        } else if (top instanceof GraphLogicalPathExpand) {
            GraphLogicalPathExpand pxd = (GraphLogicalPathExpand) top;
            return GraphLogicalPathExpand.create(
                    (GraphOptCluster) pxd.getCluster(),
                    ImmutableList.of(),
                    pxd.getInput(),
                    pxd.getExpand(),
                    pxd.getGetV(),
                    pxd.getOffset(),
                    pxd.getFetch(),
                    pxd.getResultOpt(),
                    pxd.getPathOpt(),
                    pxd.getUntilCondition(),
                    pxd.getAliasName(),
                    startAlias,
                    pxd.isOptional());
        } else if (top instanceof GraphLogicalGetV) {
            GraphLogicalGetV getV = (GraphLogicalGetV) top;
            return GraphLogicalGetV.create(
                    (GraphOptCluster) getV.getCluster(),
                    getV.getHints(),
                    getV.getInput(0),
                    getV.getOpt(),
                    getV.getTableConfig(),
                    getV.getAliasName(),
                    startAlias,
                    getV.getFilters());
        }
        logger.warn("unable to set start alias of the rel = [" + top + "]");
        return top;
    }

    static RelNode setAlias(RelNode top, String aliasName) {
        if (top instanceof GraphLogicalSource) {
            return GraphLogicalSource.create(
                    (GraphOptCluster) top.getCluster(),
                    ((GraphLogicalSource) top).getHints(),
                    ((GraphLogicalSource) top).getOpt(),
                    ((GraphLogicalSource) top).getTableConfig(),
                    aliasName,
                    ((GraphLogicalSource) top).getUniqueKeyFilters(),
                    ((GraphLogicalSource) top).getFilters());
        } else if (top instanceof GraphLogicalExpand) {
            GraphLogicalExpand expand = (GraphLogicalExpand) top;
            return GraphLogicalExpand.create(
                    (GraphOptCluster) expand.getCluster(),
                    expand.getHints(),
                    expand.getInput(0),
                    expand.getOpt(),
                    expand.getTableConfig(),
                    aliasName,
                    expand.getStartAlias(),
                    expand.isOptional(),
                    expand.getFilters(),
                    (GraphSchemaType) expand.getRowType().getFieldList().get(0).getType());
        } else if (top instanceof GraphLogicalPathExpand) {
            GraphLogicalPathExpand pxd = (GraphLogicalPathExpand) top;
            return GraphLogicalPathExpand.create(
                    (GraphOptCluster) pxd.getCluster(),
                    ImmutableList.of(),
                    pxd.getInput(),
                    pxd.getExpand(),
                    pxd.getGetV(),
                    pxd.getOffset(),
                    pxd.getFetch(),
                    pxd.getResultOpt(),
                    pxd.getPathOpt(),
                    pxd.getUntilCondition(),
                    aliasName,
                    pxd.getStartAlias(),
                    pxd.isOptional());
        } else if (top instanceof GraphLogicalGetV) {
            GraphLogicalGetV getV = (GraphLogicalGetV) top;
            return GraphLogicalGetV.create(
                    (GraphOptCluster) getV.getCluster(),
                    getV.getHints(),
                    getV.getInput(0),
                    getV.getOpt(),
                    getV.getTableConfig(),
                    aliasName,
                    getV.getStartAlias(),
                    getV.getFilters());
        }
        logger.warn("unable to set alias of the rel = [" + top + "]");
        return top;
    }

    static GraphOpt.Expand getExpandOpt(RelNode top) {
        if (top instanceof GraphLogicalExpand) {
            return ((GraphLogicalExpand) top).getOpt();
        }
        if (top instanceof GraphLogicalPathExpand) {
            return getExpandOpt(((GraphLogicalPathExpand) top).getExpand());
        }
        throw new IllegalArgumentException("unable to get expand opt from rel = [" + top + "]");
    }

    static RelNode toSource(RelNode top) {
        if (top instanceof GraphLogicalGetV) {
            return GraphLogicalSource.create(
                    (GraphOptCluster) top.getCluster(),
                    ((GraphLogicalGetV) top).getHints(),
                    GraphOpt.Source.VERTEX,
                    ((GraphLogicalGetV) top).getTableConfig(),
                    ((GraphLogicalGetV) top).getAliasName());
        }
        throw new IllegalArgumentException("unable to convert rel = [" + top + "] to source");
    }

    static RelNode reverse(RelNode top, @Nullable RelNode reversed) {
        String startAliasName = getAliasName(reversed);
        Integer startAliasId = getAliasId(reversed);
        AliasNameWithId startAlias =
                new AliasNameWithId(
                        startAliasName == null ? AliasInference.DEFAULT_NAME : startAliasName,
                        startAliasId == null ? AliasInference.DEFAULT_ID : startAliasId);
        if (top instanceof GraphLogicalSource) {
            GraphOpt.Expand expandOpt = getExpandOpt(reversed);
            GraphOpt.GetV reversedOpt;
            switch (expandOpt) {
                case BOTH:
                    reversedOpt = GraphOpt.GetV.OTHER;
                    break;
                case OUT:
                    reversedOpt = GraphOpt.GetV.END;
                    break;
                case IN:
                default:
                    reversedOpt = GraphOpt.GetV.START;
            }
            return GraphLogicalGetV.create(
                    (GraphOptCluster) top.getCluster(),
                    ((GraphLogicalSource) top).getHints(),
                    reversed,
                    reversedOpt,
                    ((GraphLogicalSource) top).getTableConfig(),
                    ((GraphLogicalSource) top).getAliasName(),
                    startAlias);
        }
        if (top instanceof GraphLogicalExpand) {
            GraphLogicalExpand expand = (GraphLogicalExpand) top;
            GraphOpt.Expand reversedOpt;
            switch (expand.getOpt()) {
                case BOTH:
                    reversedOpt = GraphOpt.Expand.BOTH;
                    break;
                case OUT:
                    reversedOpt = GraphOpt.Expand.IN;
                    break;
                case IN:
                default:
                    reversedOpt = GraphOpt.Expand.OUT;
            }
            return GraphLogicalExpand.create(
                    (GraphOptCluster) top.getCluster(),
                    ((GraphLogicalExpand) top).getHints(),
                    reversed,
                    reversedOpt,
                    ((GraphLogicalExpand) top).getTableConfig(),
                    ((GraphLogicalExpand) top).getAliasName(),
                    startAlias,
                    ((GraphLogicalExpand) top).isOptional(),
                    ((GraphLogicalExpand) top).getFilters(),
                    (GraphSchemaType) top.getRowType().getFieldList().get(0).getType());
        }
        if (top instanceof GraphLogicalPathExpand) {
            return GraphLogicalPathExpand.create(
                    (GraphOptCluster) top.getCluster(),
                    ImmutableList.of(),
                    reversed,
                    reverse(((GraphLogicalPathExpand) top).getExpand(), null),
                    reverse(((GraphLogicalPathExpand) top).getGetV(), null),
                    ((GraphLogicalPathExpand) top).getOffset(),
                    ((GraphLogicalPathExpand) top).getFetch(),
                    ((GraphLogicalPathExpand) top).getResultOpt(),
                    ((GraphLogicalPathExpand) top).getPathOpt(),
                    ((GraphLogicalPathExpand) top)
                            .getUntilCondition(), // todo: path expand with until condition cannot
                    // be reversed
                    ((GraphLogicalPathExpand) top).getAliasName(),
                    startAlias,
                    ((GraphLogicalPathExpand) top).isOptional());
        }
        if (top instanceof GraphLogicalGetV) {
            GraphLogicalGetV getV = (GraphLogicalGetV) top;
            GraphOpt.GetV reversedOpt;
            switch (getV.getOpt()) {
                case OTHER:
                    reversedOpt = GraphOpt.GetV.OTHER;
                    break;
                case BOTH:
                    reversedOpt = GraphOpt.GetV.BOTH;
                    break;
                case END:
                    reversedOpt = GraphOpt.GetV.START;
                    break;
                case START:
                default:
                    reversedOpt = GraphOpt.GetV.END;
            }
            return GraphLogicalGetV.create(
                    (GraphOptCluster) top.getCluster(),
                    ((GraphLogicalGetV) top).getHints(),
                    reversed,
                    reversedOpt,
                    ((GraphLogicalGetV) top).getTableConfig(),
                    ((GraphLogicalGetV) top).getAliasName(),
                    startAlias,
                    ((GraphLogicalGetV) top).getFilters());
        }
        logger.warn("unable to reverse the rel = [" + top + "]");
        return top;
    }

    static void getMatchBeforeJoin(RelNode top, List<GraphLogicalSingleMatch> matches) {
        if (top instanceof GraphLogicalSingleMatch) {
            matches.add(0, (GraphLogicalSingleMatch) top);
        }
        if (top.getInputs().size() != 1) return;
        getMatchBeforeJoin(top.getInput(0), matches);
    }

    static @Nullable Integer getAliasId(@Nullable RelNode rel) {
        if (rel instanceof AbstractBindableTableScan) {
            return ((AbstractBindableTableScan) rel).getAliasId();
        }
        if (rel instanceof GraphLogicalPathExpand) {
            return ((GraphLogicalPathExpand) rel).getAliasId();
        }
        return null;
    }

    static @Nullable String getAliasName(@Nullable RelNode rel) {
        if (rel instanceof AbstractBindableTableScan) {
            return ((AbstractBindableTableScan) rel).getAliasName();
        }
        if (rel instanceof GraphLogicalPathExpand) {
            return ((GraphLogicalPathExpand) rel).getAliasName();
        }
        return null;
    }

    static int getExpandCount(RelNode top) {
        int childCnt = top.getInputs().stream().mapToInt(k -> getExpandCount(k)).sum();
        if (top instanceof GraphLogicalExpand) ++childCnt;
        return childCnt;
    }

    static class SetOptional extends GraphShuttle {
        @Override
        public RelNode visit(GraphLogicalExpand expand) {
            expand = (GraphLogicalExpand) visitChildren(expand);
            RelDataType originalType = expand.getRowType().getFieldList().get(0).getType();
            GraphSchemaType schemaTypeNullable =
                    (GraphSchemaType)
                            expand.getCluster()
                                    .getTypeFactory()
                                    .createTypeWithNullability(originalType, true);
            return GraphLogicalExpand.create(
                    (GraphOptCluster) expand.getCluster(),
                    expand.getHints(),
                    expand.getInput(0),
                    expand.getOpt(),
                    expand.getTableConfig(),
                    expand.getAliasName(),
                    expand.getStartAlias(),
                    true,
                    expand.getFilters(),
                    schemaTypeNullable);
        }

        @Override
        public RelNode visit(GraphLogicalPathExpand pxd) {
            pxd = (GraphLogicalPathExpand) visitChildren(pxd);
            return GraphLogicalPathExpand.create(
                    (GraphOptCluster) pxd.getCluster(),
                    ImmutableList.of(),
                    pxd.getInput(),
                    pxd.getExpand(),
                    pxd.getGetV(),
                    pxd.getOffset(),
                    pxd.getFetch(),
                    pxd.getResultOpt(),
                    pxd.getPathOpt(),
                    pxd.getUntilCondition(),
                    pxd.getAliasName(),
                    pxd.getStartAlias(),
                    true);
        }
    }

    static class Reverse extends GraphShuttle {
        private final RelNode top;
        private RelNode reversed;

        public Reverse(RelNode top) {
            this.top = top;
        }

        @Override
        public RelNode visit(GraphLogicalSource source) {
            if (top == source) return source;
            return reverse(source, reversed);
        }

        @Override
        public RelNode visit(GraphLogicalExpand expand) {
            reversed = reverse(expand, reversed);
            return expand.getInput(0).accept(this);
        }

        @Override
        public RelNode visit(GraphLogicalGetV getV) {
            reversed = (getV == top) ? toSource(getV) : reverse(getV, reversed);
            return getV.getInput(0).accept(this);
        }

        @Override
        public RelNode visit(GraphLogicalPathExpand pxd) {
            reversed = reverse(pxd, reversed);
            return pxd.getInput(0).accept(this);
        }
    }

    static class ReplaceInput extends GraphShuttle {
        private final AliasNameWithId inputAlias;
        private final RelNode input;

        public ReplaceInput(AliasNameWithId inputAlias, RelNode input) {
            this.inputAlias = inputAlias;
            this.input = input;
        }

        @Override
        public RelNode visit(GraphLogicalExpand expand) {
            boolean resetAlias =
                    !expand.getInputs().isEmpty()
                            && expand.getInput(0) instanceof GraphLogicalSource;
            expand = (GraphLogicalExpand) visitChildren(expand);
            return resetAlias ? setStartAlias(expand, inputAlias) : expand;
        }

        @Override
        public RelNode visit(GraphLogicalPathExpand pxd) {
            boolean resetAlias =
                    !pxd.getInputs().isEmpty() && pxd.getInput(0) instanceof GraphLogicalSource;
            pxd = (GraphLogicalPathExpand) visitChildren(pxd);
            return resetAlias ? setStartAlias(pxd, inputAlias) : pxd;
        }

        @Override
        public RelNode visit(GraphLogicalSource source) {
            return input;
        }
    }
}