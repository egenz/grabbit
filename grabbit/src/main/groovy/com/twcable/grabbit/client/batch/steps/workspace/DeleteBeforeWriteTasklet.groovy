package com.twcable.grabbit.client.batch.steps.workspace

import com.twcable.grabbit.jcr.JcrUtil
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.sling.jcr.api.SlingRepository
import org.springframework.batch.core.StepContribution
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.repeat.RepeatStatus

import javax.jcr.Node
import javax.jcr.Session

/*
 * Copyright 2015 Time Warner Cable, Inc.
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

@Slf4j
@CompileStatic
class DeleteBeforeWriteTasklet implements Tasklet {

    private String jobPath

    private Collection<String> excludePaths

    private SlingRepository slingRepository

    void setExcludePaths(String paths) {
        final thisPathsString = paths?.trim()
        if(!thisPathsString) {
            excludePaths = []
            return
        }
        Collection<String> theseExcludePaths = thisPathsString.split("\\*") as Collection
        excludePaths = theseExcludePaths.collect { String excludePath ->
            return (excludePath[0] != '/') ? "/${excludePath}".toString() : excludePath
        }
    }


    void setJobPath(String jobPath) {
        String thisJobPath = jobPath.trim()
        thisJobPath = thisJobPath[0] != '/' ? "/${thisJobPath}" : thisJobPath
        this.jobPath = thisJobPath
    }


    void setSlingRepository(SlingRepository slingRepository) {
        this.slingRepository = slingRepository
    }

    /**
     * Given the current context in the form of a step contribution, do whatever
     * is necessary to process this unit inside a transaction. Implementations
     * return {@link RepeatStatus#FINISHED} if finished. If not they return
     * {@link RepeatStatus#CONTINUABLE}. On failure throws an exception.
     *
     * @param contribution mutable state to be passed back to update the current
     * step execution
     * @param chunkContext attributes shared between invocations but not between
     * restarts
     * @return an {@link RepeatStatus} indicating whether processing is
     * continuable.
     */
    @Override
    RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        final Session session = JcrUtil.getSession(slingRepository, "admin")
        //If we don't have any exclude paths, we don't have to worry about only clearing particular subtrees
        if(!excludePaths || excludePaths.empty) {
            final Node node = session.getNode(jobPath)
            node.remove()
        }
        else {
            deletePartialTree(session)
        }
        session.save()
        return RepeatStatus.FINISHED
    }


    private void deletePartialTree(final Session session) {
        final relativeExcludePaths = excludePaths.collect { it - jobPath }
        final relativeRoot = session.getNode(jobPath)
        deletePartialTree(relativeRoot, relativeExcludePaths)
    }


    private void deletePartialTree(Node rootNode, Collection<String> relativeExcludePaths) {
        //Base case.  If a root node does not have any exclude paths under it, it must have been excluded
        if(!relativeExcludePaths) {
            return
        }
        //Compute the current tree level excluded nodes, and compute the remaining paths to traverse
        Collection<NodeAndExcludePaths> nodeAndExcludePaths = relativeExcludePaths.inject([] as Collection<NodeAndExcludePaths>) { def acc, def thisPath ->
            final thisNodeAndExcludePath = NodeAndExcludePaths.fromPath(thisPath)
            //If we have already created a nodeAndExcludePath for this node name, just add to it's exclude paths
            final matchingNodeName = acc.find { it.nodeName == thisNodeAndExcludePath.nodeName }
            if(matchingNodeName) {
                matchingNodeName.excludePaths.addAll(thisNodeAndExcludePath.excludePaths)
            }
            else {
                acc.add(thisNodeAndExcludePath)
            }
            return acc
        } as Collection
        //Delete nodes allowed under this tree
        final rootNodeChildren = rootNode.getNodes()
        while(rootNodeChildren.hasNext()) {
            final currentNode = rootNodeChildren.nextNode()
            //If this node is in our exclusion list, don't delete it
            if(!(nodeAndExcludePaths.find { currentNode.name == it.nodeName })) {
                currentNode.remove()
            }
        }
        //Recurse on each
        nodeAndExcludePaths.each {
            deletePartialTree(rootNode.getNode(it.nodeName), it.excludePaths)
        }
    }

    /**
     * If we have a path for a node such as 'foo/bar/doo'
     * node is 'foo', and the excludePaths is '/bar/doo'
     */
    static class NodeAndExcludePaths {

        String nodeName
        Collection<String> excludePaths

        static NodeAndExcludePaths fromPath(final String path) {
            //Remove leading '/'
            final String thisPath = path.replaceFirst('/', '')
            //We need to find the current node, and the remaining path e.g 'foo/bar/doo', node: foo, remaining path: /bar/doo
            final slashIndex = thisPath.indexOf('/')
            //There is no remaining path
            if(slashIndex == -1) {
                return new NodeAndExcludePaths(nodeName: thisPath, excludePaths: [])
            }
            final remainingPath = thisPath.substring(slashIndex)
            final node = thisPath - remainingPath
            return new NodeAndExcludePaths(nodeName: node, excludePaths: [remainingPath])
        }
    }

}
