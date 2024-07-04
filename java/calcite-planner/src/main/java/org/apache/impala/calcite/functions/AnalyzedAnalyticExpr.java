// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.calcite.functions;

import java.util.List;
import org.apache.impala.analysis.AnalyticExpr;
import org.apache.impala.analysis.AnalyticWindow;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.OrderByElement;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;

/**
 * Analytical Expression that is always in analyzed state
 */
public class AnalyzedAnalyticExpr extends AnalyticExpr {
  public AnalyzedAnalyticExpr(FunctionCallExpr fnCall,
      List<Expr> partitionExprs, List<OrderByElement> orderByElements,
      AnalyticWindow window) throws ImpalaException {
    super(fnCall, partitionExprs, orderByElements, window);
    this.type_ = fnCall.getType();
  }

  public AnalyzedAnalyticExpr(AnalyzedAnalyticExpr other) {
    super(other);
    this.type_ = other.type_;
  }

  @Override
  public Expr clone() {
    return new AnalyzedAnalyticExpr(this);
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    // TODO: Will need to call standardize for some of the analytic functions.
  }
}
