/*
 * This file is part of cBioPortal.
 *
 * cBioPortal is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.mskcc.cbio.portal.scripts;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoGeneticAlteration;
import org.mskcc.cbio.portal.dao.DaoGeneticProfile;
import org.mskcc.cbio.portal.dao.DaoGeneticProfileSamples;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.model.GeneticProfile;
import org.mskcc.cbio.portal.model.Sample;
import org.mskcc.cbio.portal.util.ProgressMonitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.cbioportal.web.parameter.sort.SampleSortBy.sampleId;

/**
 * Command Line Tool to Remove Samples in Cancer Studies
 */
public class RemoveSamples extends ConsoleRunnable {

    public static final String COMMA = ",";
    private Set<String> studyIds;
    private Set<String> sampleIds;

    public void run() {
        parseArgs();
        ProgressMonitor.setCurrentMessage("Removing sample id(s) ("
                + String.join(", ", sampleIds)
                + ") from study(ies) with id(s) ("
                + String.join(", ", studyIds) + ")...");
        ProgressMonitor.logDebug("Reading study id(s) from the database.");
        final Set<Integer> internalStudyIds = studyIds.stream().map(studyId -> {
            try {
                return DaoCancerStudy.getCancerStudyByStableId(studyId).getInternalId();
            } catch (DaoException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toUnmodifiableSet());
        ProgressMonitor.logDebug("Found study(ies) with the following internal id(s):" + internalStudyIds.stream().map(id -> Integer.toString(id)).collect(Collectors.joining(", ")));
        ProgressMonitor.logDebug("Reading internal sample id(s) from the database.");
        final Map<Integer, Set<Integer>> internalStudyIdToInternalSampleIds = new HashMap<>();
        for (Integer internalStudyId : internalStudyIds) {
            ProgressMonitor.logDebug("Searching for samples in study with internal id=" + internalStudyId);
            HashSet<Integer> internalSampleIds = new HashSet<>();
            internalStudyIdToInternalSampleIds.put(internalStudyId, internalSampleIds);
            for (String sampleId : sampleIds) {
                Sample sampleByCancerStudyAndSampleId = DaoSample.getSampleByCancerStudyAndSampleId(internalStudyId, sampleId);
                if (sampleByCancerStudyAndSampleId != null) {
                    internalSampleIds.add(sampleByCancerStudyAndSampleId.getInternalId());
                }
            }
            ProgressMonitor.logDebug("Found sample(s) with the following internal id(s):" + internalSampleIds.stream().map(id -> Integer.toString(id)).collect(Collectors.joining(", ")));
        }
        try {
            for (Integer internalStudyId : internalStudyIds) {
                ProgressMonitor.logDebug("Removing samples in study with internal id=" + internalStudyId);
                Set<Integer> internalSampleIds = internalStudyIdToInternalSampleIds.get(internalStudyId);
                List<GeneticProfile> geneticProfiles = DaoGeneticProfile.getAllGeneticProfiles(internalStudyId);
                for (GeneticProfile geneticProfile : geneticProfiles) {
                    List<Integer> orderedSampleList = DaoGeneticProfileSamples.getOrderedSampleList(geneticProfile.getGeneticProfileId());
                    if (orderedSampleList.removeAll(internalSampleIds)) {
                        ProgressMonitor.logDebug("There are samples to delete for genetic profile with the stable id=" + geneticProfile.getStableId());

                        HashMap<Integer, HashMap<Integer, String>> geneticAlterationMapForEntityIds = DaoGeneticAlteration.getInstance().getGeneticAlterationMapForEntityIds(geneticProfile.getGeneticProfileId(), null);
                        for (Map.Entry<Integer, HashMap<Integer, String>> entry: geneticAlterationMapForEntityIds.entrySet()) {
                            DaoGeneticAlteration.getInstance().deleteAllRecordsInGeneticProfile(geneticProfile.getGeneticProfileId(), entry.getKey());
                            if (!orderedSampleList.isEmpty()) {
                                String[] values = orderedSampleList.stream().map(isid -> entry.getValue().get(isid)).toArray(String[]::new);
                                DaoGeneticAlteration.getInstance().addGeneticAlterationsForGeneticEntity(geneticProfile.getGeneticProfileId(), entry.getKey(), values);
                            }
                        }
                        DaoGeneticProfileSamples.deleteAllSamplesInGeneticProfile(geneticProfile.getGeneticProfileId());
                        if (!orderedSampleList.isEmpty()) {
                            DaoGeneticProfileSamples.addGeneticProfileSamples(geneticProfile.getGeneticProfileId(), orderedSampleList);
                        }
                    } else {
                        ProgressMonitor.logDebug("No samples to delete for genetic profile with the stable id=" + geneticProfile.getStableId());
                    }
                }
                ProgressMonitor.logDebug("Deleting samples from the rest of the tables for study with internal id=" + internalStudyId);
                DaoSample.deleteSamples(internalSampleIds);
            }
        } catch (DaoException e) {
            throw new RuntimeException(e);
        }
        ProgressMonitor.setCurrentMessage("Done");
    }

    private void parseArgs() {
        OptionParser parser = new OptionParser();
        OptionSpec<String> studyIdsOpt = parser.accepts("study_ids", "Cancer Study ID(s; comma separated) to remove samples for.")
                .withRequiredArg()
                .describedAs("comma separated study ids")
                .ofType(String.class);
        OptionSpec<String> sampleIdsOpt = parser.accepts("sample_ids", "Samples Stable ID(s; comma separated) to remove.")
                .withRequiredArg()
                .describedAs("comma separated sample ids")
                .ofType(String.class);
        OptionSpec<Void> help = parser.accepts("help", "print this help info");
        String progName = this.getClass().getSimpleName();
        String description = "Removes clinical sample(s) information by their stable id(s) and cancer study id(s).";

        OptionSet options;
        try {
            options = parser.parse(args);
        } catch (OptionException e) {
            throw new UsageException(progName, description, parser,
                    e.getMessage());
        }

        if (options.has(help)) {
            throw new UsageException(progName, description, parser);
        }
        if (!options.has(studyIdsOpt) || options.valueOf(studyIdsOpt) == null || "".equals(options.valueOf(studyIdsOpt).trim())) {
            throw new UsageException(progName, description, parser, "'--study_ids' argument has to specify study id(s).");
        }
        if (!options.has(sampleIdsOpt) || options.valueOf(sampleIdsOpt) == null || "".equals(options.valueOf(sampleIdsOpt).trim())) {
            throw new UsageException(progName, description, parser, "'--sample_ids' argument has to specify sample id(s).");
        }
        this.studyIds = parseCsvAsSet(options.valueOf(studyIdsOpt));
        this.sampleIds = parseCsvAsSet(options.valueOf(sampleIdsOpt));
    }

    @NotNull
    private Set<String> parseCsvAsSet(String s) {
        return Arrays.stream(s.trim().split(COMMA)).filter(val -> !"".equals(val)).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Makes an instance to run with the given command line arguments.
     *
     * @param args the command line arguments to be used
     */
    public RemoveSamples(String[] args) {
        super(args);
    }

    /**
     * Runs the command as a script and exits with an appropriate exit code.
     *
     * @param args the arguments given on the command line
     */
    public static void main(String[] args) {
        ConsoleRunnable runner = new RemoveSamples(args);
        runner.runInConsole();
    }
}
